from multiprocessing.connection import Client, Listener
from queue import Queue
from socket import socket
import threading
import time



class MPParentInterface(object):
    def __init__(self, record_q, record_addr, response_q, response_addr):
        self.record_q = record_q
        self.record_addr = record_addr
        self.response_q = response_q
        self.response_addr = response_addr
        self._record_listener = Listener(record_addr)
        self._record_conn = self._record_listener.accept()
        self._response_client = Client(response_addr)
        self._response_thread = threading.Thread(target=self._response_loop)
        self._response_thread.daemon = True
        self._record_thread = threading.Thread(target=self._loop)
        self._record_thread.daemon = True
        self._record_thread.start()
        self._response_thread.start()

    def _record_loop(self):
        while True:
            try:
                rec = self.conn.recv()
                if rec == "shutdown":
                    break
                self.record_q.put(rec)
            except Exception:
                pass

    def _response_loop(self):
        while True:
            try:
                rec = self.response_q.get()
                self._response_client.send(rec)
            except Exception:
                pass


    def close(self):
        client = Client(self.record_addr)
        client.send("close")
        time.sleep(0.5)
        self._record_conn.close()
        self._record_listener.close()
        client.close()
        self._response_client.close()


class MPChildInterface(object):
    def __init__(self, request_addr, response_addr):
        self.request_addr = request_addr
        self._request_client = Client(request_addr)
        self.response_addr = response_addr
        self._response_listener = Listener(response_addr)
        self._response_conn = self._response_listener.accept()
        self._response_q = Queue()

    def request_q_put(self, rec):
        self._request_client.send(rec)

    def response_q_get(self):
        rec = self._response_conn.recv()
        return rec

    def close(self):
        self._response_conn.close()
        self._response_listener.close()
        self._request_client.close()


def _get_free_port():
    with socket() as s:
        s.bind(('',0))
        return s.getsockname()[1]
