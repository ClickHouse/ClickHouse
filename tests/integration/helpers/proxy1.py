import socket
import threading


# simple one-connection proxy with PROXY v1 protocol
# https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
class Proxy1:
    def __init__(self, proxy_string=""):
        self._proxy_string = proxy_string

    def _run(self):
        self._server, addr = self._sock.accept()
        self._sock.close()
        self._client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._client.bind(("", 0))
        self._client.connect(self._address)
        self._client.send("PROXY ".encode("utf-8"))
        if self._proxy_string == "":
            self._client.send(
                (
                    "TCP4 "
                    + addr[0]
                    + " "
                    + self._address[0]
                    + " "
                    + str(addr[1])
                    + " "
                    + str(self._address[1])
                ).encode("utf-8")
            )
        else:
            self._client.send(self._proxy_string.encode("utf-8"))
        self._client.send("\r\n".encode("utf-8"))

        def forward(source: socket.socket, destination: socket.socket):
            while True:
                try:
                    data = source.recv(4096)
                    if not data:
                        destination.shutdown(socket.SHUT_WR)
                        source.shutdown(socket.SHUT_RD)
                        break
                    destination.sendall(data)
                except Exception:
                    break

        client_to_server_thread = threading.Thread(
            target=forward, args=(self._client, self._server)
        )
        server_to_client_thread = threading.Thread(
            target=forward, args=(self._server, self._client)
        )

        client_to_server_thread.start()
        server_to_client_thread.start()
        client_to_server_thread.join()
        server_to_client_thread.join()

        self._client.close()
        self._server.close()

    def start(self, address):
        self._address = address
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("", 0))
        self._sock.listen(1)
        self._runner = threading.Thread(target=self._run)
        self._runner.start()
        return self._sock.getsockname()[1]

    def wait(self):
        if self._runner:
            self._runner.join()

    def stop(self):
        if self._sock:
            self._sock.close()
        if self._client:
            self._client.close()
        if self._server:
            self._server.close()
        self.wait()
