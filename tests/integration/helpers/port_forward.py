import socket
import threading
from typing import Any, Callable


# simple port forwarding
class PortForward:
    def __init__(self) -> None:
        self._clients_lock: threading.Lock = threading.Lock()
        self._clients: dict[socket.socket, tuple[socket.socket,
                                                 threading.Thread, threading.Thread]] = {}
        self._address: tuple[str, int] | None = None
        self._sock: socket.socket | None = None
        self._runner: threading.Thread | None = None

    def _run(self) -> None:
        while True:
            try:
                assert self._sock is not None
                connection, addr = self._sock.accept()
            except socket.timeout:
                continue
            except Exception:
                break

            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.bind(("", 0))
            client.connect(self._address)

            connection.settimeout(1)
            client.settimeout(1)

            def forward(source: socket.socket, destination: socket.socket, terminate: Callable[[], None]) -> None:
                while True:
                    try:
                        data = source.recv(4096)
                        if not data:
                            destination.shutdown(socket.SHUT_WR)
                            source.shutdown(socket.SHUT_RD)
                            break
                        sent = 0
                        while sent < len(data):
                            try:
                                sent += destination.send(data[sent:])
                            except socket.timeout:
                                continue
                    except socket.timeout:
                        continue
                    except Exception:
                        break

                terminate()

            def termination(connection: socket.socket, idx: int) -> None:
                client_tuple: tuple[socket.socket,
                                    threading.Thread, threading.Thread] | None = None
                with self._clients_lock:
                    client_tuple = self._clients.pop(connection, None)
                if client_tuple is not None:
                    client_tuple[idx].join()
                    connection.close()
                    client_tuple[0].close()

            client_to_server_thread = threading.Thread(
                target=forward, args=(connection, client, lambda: termination(connection, 2))
            )
            server_to_client_thread = threading.Thread(
                target=forward, args=(client, connection, lambda: termination(connection, 1))
            )

            with self._clients_lock:
                self._clients[connection] = (client, client_to_server_thread, server_to_client_thread)

            client_to_server_thread.start()
            server_to_client_thread.start()

    def start(self, address: tuple[str, int], listen_port: int = 0) -> int:
        self._address = address
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("", listen_port))
        self._sock.listen()
        self._sock.settimeout(1)
        self._runner = threading.Thread(target=self._run)
        self._runner.start()
        return self._sock.getsockname()[1]

    def stop(self, force: bool = False) -> None:
        if self._sock:
            self._sock.close()

        if not force:
            return

        if self._runner:
            self._runner.join()

        while True:
            item = None
            with self._clients_lock:
                try:
                    item = self._clients.popitem()
                except KeyError:
                    break
            
            (connection, (client, client_to_server_thread, server_to_client_thread)) = item

            connection.close()
            client.close()
            client_to_server_thread.join()
            server_to_client_thread.join()
