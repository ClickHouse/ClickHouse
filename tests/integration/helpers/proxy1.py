import socket
import threading

class Proxy1:
    def __init__(
        self,
        proxy_string=""
    ):
        self.proxy_string = proxy_string

    def run(self):
        self.server, addr = self.sock.accept()
        self.sock.close()
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.bind(("", 0))
        self.client.connect(self.address)
        self.client.send("PROXY ".encode("utf-8"))
        if self.proxy_string == "":
            self.client.send(("TCP4 " + addr[0] + " " + self.address[0] + " " + str(addr[1]) + " " + str(self.address[1])).encode("utf-8"))
        else:
            self.client.send(self.proxy_string.encode("utf-8"))
        self.client.send("\r\n".encode("utf-8"))

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

        client_to_server_thread = threading.Thread(target=forward, args=(self.client, self.server))
        server_to_client_thread = threading.Thread(target=forward, args=(self.server, self.client))

        client_to_server_thread.start()
        server_to_client_thread.start()
        client_to_server_thread.join()
        server_to_client_thread.join()

        self.client.close()
        self.server.close()

    def start(self, address):
        self.address = address
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(("", 0))
        self.sock.listen(1)
        self.runner = threading.Thread(target=self.run)
        self.runner.start()
        return self.sock.getsockname()[1]

    def wait(self):
        if self.runner:
            self.runner.join()

    def stop(self):
        if self.sock:
            self.sock.close()
        if self.client:
            self.client.close()
        if self.server:
            self.server.close()
        self.wait()
