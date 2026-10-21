#!/usr/bin/env python3
"""A NATS proxy which rejects credentials on demand, used to drive a JetStream table through a
credential rotation against a real broker.

`nats_fake_broker.py` speaks just enough core NATS to reject the credentials of a live table,
but JetStream needs a real broker: the stream, the durable consumer, the ack accounting and the
redelivery all live on the server. A real broker cannot be made to reject the credentials of an
already connected client without a restart, which the integration harness does not allow for
the `nats1` container, so this sits between the table and the broker instead. It relays the
protocol verbatim except for the very first exchange: it answers `INFO` on behalf of the broker
and decides from the state file whether to forward the `CONNECT` line or to answer it with
`-ERR 'Authorization Violation'`, exactly the way a broker does after a rotation.

The broker requires TLS and the proxy terminates it: the table connects to the proxy in plain
text, the proxy connects to the broker over TLS.

Usage: nats_auth_proxy.py <port> <state file> <upstream host> <upstream port>

The state file holds either `accept` or `reject`. While it holds `reject`, every `CONNECT` is
answered with `-ERR 'Authorization Violation'` and every live connection is dropped.
"""

import json
import socket
import ssl
import sys
import threading
import time

STATE_ACCEPT = "accept"
STATE_REJECT = "reject"

AUTHORIZATION_ERR = b"-ERR 'Authorization Violation'\r\n"


def log(message):
    print("{:.3f} {}".format(time.time(), message), flush=True)


def read_line(connection):
    """Reads one `\\r\\n`-terminated line, returning it together with whatever followed it."""
    buffer = b""
    while b"\r\n" not in buffer:
        data = connection.recv(4096)
        if not data:
            raise OSError("the peer closed the connection before sending a line")
        buffer += data
    line, rest = buffer.split(b"\r\n", 1)
    return line, rest


class AuthProxy:
    def __init__(self, port, state_path, upstream_host, upstream_port):
        self.port = port
        self.state_path = state_path
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port
        self.lock = threading.Lock()
        # Every accepted client connection, with the broker connection it is relayed to.
        self.clients = {}
        # The broker's certificate is self-signed by the test CA and the table does not verify
        # it either, so neither does the proxy.
        self.tls_context = ssl.create_default_context()
        self.tls_context.check_hostname = False
        self.tls_context.verify_mode = ssl.CERT_NONE

    def state(self):
        # A missing file reads as `accept`, so the proxy is usable before a test writes any state.
        try:
            with open(self.state_path) as state_file:
                return state_file.read().strip() or STATE_ACCEPT
        except FileNotFoundError:
            return STATE_ACCEPT

    def run(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", self.port))
        listener.listen(16)
        log(
            "listening on port {}, state file {}, upstream {}:{}".format(
                self.port, self.state_path, self.upstream_host, self.upstream_port
            )
        )

        threading.Thread(target=self.enforce_state, daemon=True).start()

        while True:
            connection, _ = listener.accept()
            threading.Thread(target=self.serve, args=(connection,), daemon=True).start()

    def connect_upstream(self):
        # The broker sends `INFO` in plain text and only then expects the TLS handshake.
        upstream = socket.create_connection((self.upstream_host, self.upstream_port))
        info_line, rest = read_line(upstream)
        if rest:
            raise OSError("the broker sent more than an INFO line before the TLS handshake")
        upstream = self.tls_context.wrap_socket(upstream, server_hostname=self.upstream_host)
        return upstream, info_line

    def rewrite_info(self, info_line):
        info = json.loads(info_line[len(b"INFO ") :])
        # The client talks to the proxy in plain text, and must not learn the broker's own
        # address, otherwise it would reconnect there directly and bypass the rejection.
        info["tls_required"] = False
        info["tls_available"] = False
        info.pop("connect_urls", None)
        return b"INFO " + json.dumps(info).encode() + b"\r\n"

    def serve(self, client):
        upstream = None
        try:
            upstream, info_line = self.connect_upstream()
            client.sendall(self.rewrite_info(info_line))

            connect_line, rest = read_line(client)
            if not connect_line.upper().startswith(b"CONNECT"):
                raise OSError("the client sent {!r} instead of CONNECT".format(connect_line))

            if self.state() == STATE_REJECT:
                log("rejecting the credentials of a connection")
                client.sendall(AUTHORIZATION_ERR)
                return

            log("credentials accepted")
            with self.lock:
                self.clients[client] = upstream
            upstream.sendall(connect_line + b"\r\n" + rest)

            relay = threading.Thread(target=self.relay, args=(upstream, client, "broker"), daemon=True)
            relay.start()
            self.relay(client, upstream, "client")
            relay.join()
        except OSError as error:
            log("connection is gone: {}".format(error))
        finally:
            with self.lock:
                self.clients.pop(client, None)
            for connection in (client, upstream):
                if connection is not None:
                    connection.close()

    def relay(self, source, destination, source_name):
        try:
            while True:
                data = source.recv(65536)
                if not data:
                    break
                destination.sendall(data)
        except OSError as error:
            log("relaying from the {} stopped: {}".format(source_name, error))
        # An `EOF` on one side ends the connection on the other side too, so that the relay in
        # the opposite direction stops as well.
        for connection in (source, destination):
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def enforce_state(self):
        # A connection accepted just before the state changed has to be dropped too, otherwise
        # the client would keep consuming through it and never see the rejection.
        while True:
            if self.state() == STATE_REJECT:
                with self.lock:
                    connections = list(self.clients.items())
                    self.clients.clear()
                for client, upstream in connections:
                    log("dropping a live connection")
                    for connection in (client, upstream):
                        try:
                            connection.shutdown(socket.SHUT_RDWR)
                        except OSError as error:
                            log("cannot drop a connection: {}".format(error))
            time.sleep(0.1)


if __name__ == "__main__":
    proxy = AuthProxy(
        port=int(sys.argv[1]),
        state_path=sys.argv[2],
        upstream_host=sys.argv[3],
        upstream_port=int(sys.argv[4]),
    )
    proxy.run()
