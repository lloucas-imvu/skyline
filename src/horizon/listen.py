import socket
import errno
import time
import traceback
from os import kill, getpid
from Queue import Full
from multiprocessing import Process
from struct import Struct, unpack
from msgpack import unpackb
from cPickle import loads

import logging
import settings

logger = logging.getLogger("HorizonLog")


class Listen(Process):
    """
    The listener is responsible for listening on a port.
    """
    def __init__(self, port, queue, parent_pid, type="pickle"):
        super(Listen, self).__init__()
        try:
            self.ip = settings.HORIZON_IP
        except AttributeError:
            # Default for backwards compatibility
            self.ip = socket.gethostname()
        self.port = port
        self.q = queue
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.type = type

    def gen_unpickle(self, infile):
        """
        Generate a pickle from a stream
        """
        try:
            bunch = loads(infile)
            yield bunch
        except EOFError:
            return

    def read_all(self, sock, n):
        """
        Read n bytes from a stream
        """
        data = ''
        while n > 0:
            buf = sock.recv(n)
            n -= len(buf)
            data += buf
        return data

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def listen_pickle(self):
        """
        Listen for pickles over tcp
        """
        while 1:
            try:
                # Set up the TCP listening socket
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind((self.ip, self.port))
                s.setblocking(1)
                s.listen(5)
                logger.info('listening over tcp for pickles on %s' % self.port)

                (conn, address) = s.accept()
                logger.info('connection from %s:%s' % (address[0], self.port))

                chunk = []
                while 1:
                    self.check_if_parent_is_alive()
                    try:
                        length = Struct('!I').unpack(self.read_all(conn, 4))
                        body = self.read_all(conn, length[0])

                        # Iterate and chunk each individual datapoint
                        for bunch in self.gen_unpickle(body):
                            for metric in bunch:
                                chunk.append(metric)

                                # Queue the chunk and empty the variable
                                if len(chunk) > settings.CHUNK_SIZE:
                                    try:
                                        self.q.put(list(chunk), block=False)
                                        chunk[:] = []

                                    # Drop chunk if queue is full
                                    except Full:
                                        logger.info('queue is full, dropping datapoints')
                                        chunk[:] = []

                    except Exception as e:
                        logger.info(e)
                        logger.info('incoming connection dropped, attempting to reconnect')
                        break

            except Exception as e:
                logger.info('can\'t connect to socket: ' + str(e))
                break

    def listen_udp(self):
        """
        Listen over udp for MessagePack strings
        """
        while 1:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.bind((self.ip, self.port))
                logger.info('listening over udp for messagepack on %s' % self.port)

                chunk = []
                while 1:
                    self.check_if_parent_is_alive()
                    data, addr = s.recvfrom(1024)
                    metric = unpackb(data)
                    chunk.append(metric)

                    # Queue the chunk and empty the variable
                    if len(chunk) > settings.CHUNK_SIZE:
                        try:
                            self.q.put(list(chunk), block=False)
                            chunk[:] = []

                        # Drop chunk if queue is full
                        except Full:
                            logger.info('queue is full, dropping datapoints')
                            chunk[:] = []

            except Exception as e:
                logger.info('can\'t connect to udp socket: ' + str(e))
                break

    def listen_istatd(self):
        """
        Listen over udp for MessagePack strings
        """
        logger.info("Starting an istad listener")
        while 1:
            try:
                # Set up the TCP listening socket
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                logger.debug("ip {i} port {p}".format(i=self.ip, p=self.port))
                s.bind((self.ip, self.port))
                s.setblocking(1)
                s.listen(5)
                logger.debug('listening over tcp for istatd on %s %s' % (self.ip, self.port))

                try:
                    logger.debug('Attempting to accept')
                    (conn, address) = s.accept()
                    logger.debug('connection from %s:%s' % (address[0], self.port))
                except socket.error as e:
                    if e.errno == errno.EWOULDBLOCK:
                        logger.debug('Stuck waiting :(')
                        s.close()
                        s = None
                        time.sleep(.1) 
                        continue
                    logger.debug('Real error in accept {e}'.format(e=e))
                    raise
                except Exception as e:
                    logger.debug('Real2 error in accept {e}'.format(e=e))


                chunk = []
                rem = ""
                while 1:
                    self.check_if_parent_is_alive()
                    logger.debug('Looping for istatd data from connection')
                    logger.debug('Chunk at top {c}'.format(c=chunk))
                    try:
                        data = conn.recv(16)
                        if not data:
                            break
                        logger.debug('data {data} received'.format(data=data))
                    except socket.error as e:
                        if e.errno == 11:
                            continue
                        logger.debug('Crash {e}'.format(e=e))
                        raise

                    data2 = rem + data
                    rem = ""

                    met = data2.splitlines()

                    mets = met
                    if not data2.endswith('\n'):
                        mets = met[:-1]
                        rem = met[-1:][0]

                    logger.debug('met {met} received app'.format(met=mets))


                    for m in filter(lambda x: not x.startswith('#'), mets):
                        ##parse istatd counter.name timestamp sum sumSquared min max count 
                        mm = m.split(' ')
                        div = mm[0].startswith('*')
                        if len(mm) == 7:
                            val = float(mm[2]) / float(mm[6])
                            val = val / 10 if div else val
                            mmm = (mm[0], [int(mm[1]), val])
                        elif len(mm) == 3:
                            val = float(mm[2])
                            val = val / 10 if div else val
                            mmm = (mm[0], [int(mm[1]), val])
                        elif len(mm) == 2:
                            val = float(mm[1])
                            val = val / 10 if div else val
                            mmm = (mm[0], [int(time.time()), val])
                        else:
                            continue 
                        chunk.append(mmm)


                    logger.debug("Chunks {c}".format(c=chunk))
                    # Queue the chunk and empty the variable
                    if len(chunk) > settings.CHUNK_SIZE:
                        try:
                            logger.debug("Queueing the chunks {c}".format(c=chunk))
                            self.q.put(list(chunk), block=False)
                            chunk[:] = []

                        # Drop chunk if queue is full
                        except Full:
                            logger.info('queue is full, dropping datapoints')
                            chunk[:] = []

            except Exception as e:
                logger.info('can\'t connect to istatd tcp socket: ' + str(e))
                logger.debug(traceback.format_exc())
                break

    def run(self):
        """
        Called when process intializes.
        """
        logger.info('started listener')

        if self.type == 'pickle':
            self.listen_pickle()
        elif self.type == 'udp':
            self.listen_udp()
        elif self.type == 'istatd':
            self.listen_istatd()
        else:
            logging.error('unknown listener format')
