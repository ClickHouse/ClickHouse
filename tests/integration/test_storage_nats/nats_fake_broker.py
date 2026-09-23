#!/usr/bin/env python3
"""A minimal NATS broker, used to drive a `NATS` table through a credential rotation.

A real broker cannot be made to reject the credentials of an already connected client without
restarting it with a different configuration, which the integration harness does not allow for
the `nats1` container. This speaks just enough of the NATS protocol for the client library the
`NATS` engine uses - `INFO` on accept, `PONG` for every `PING`, and a `MSG` for every
subscription - and decides whether to accept a connection from the state file it polls, so a
test can reject the credentials of a live table exactly the way a broker does after a rotation.

Usage: nats_fake_broker.py <port> <state file> [<publish interval, seconds>]

The state file holds either `accept` or `reject`. While it holds `reject`, every `CONNECT` is
answered with `-ERR 'Authorization Violation'` and every live connection is dropped, which is
what a broker sends to a client whose password no longer matches.
"""

import json
import socket
import sys
import threading
import time

STATE_ACCEPT = "accept"
STATE_REJECT = "reject"

AUTHORIZATION_ERR = b"-ERR 'Authorization Violation'\r\n"
PONG = b"PONG\r\n"


def log(message):
    print("{:.3f} {}".format(time.time(), message), flush=True)


class FakeBroker:
    def __init__(self, port, state_path, publish_interval):
        self.port = port
        self.state_path = state_path
        self.publish_interval = publish_interval
        self.lock = threading.Lock()
        # Every connection whose credentials were accepted, with the subscriptions it has
        # declared: {socket: {sid: subject}}
        self.clients = {}
        self.published = 0

    def state(self):
        # A missing file reads as `accept`, so the broker is usable before a test writes any state.
        try:
            with open(self.state_path) as state_file:
                return state_file.read().strip() or STATE_ACCEPT
        except FileNotFoundError:
            return STATE_ACCEPT

    def info_line(self):
        info = {
            "server_id": "NATS_FAKE_BROKER",
            "server_name": "nats_fake_broker",
            "version": "2.10.0",
            "proto": 1,
            "host": "127.0.0.1",
            "port": self.port,
            "headers": True,
            "auth_required": True,
            "tls_required": False,
            "tls_available": False,
            "max_payload": 1048576,
            "client_id": 1,
        }
        return b"INFO " + json.dumps(info).encode() + b"\r\n"

    def run(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", self.port))
        listener.listen(16)
        log("listening on port {}, state file {}".format(self.port, self.state_path))

        threading.Thread(target=self.enforce_state, daemon=True).start()
        threading.Thread(target=self.publish_loop, daemon=True).start()

        while True:
            connection, _ = listener.accept()
            threading.Thread(target=self.serve, args=(connection,), daemon=True).start()

    def serve(self, connection):
        # The client reads the whole first line and discards whatever followed it in the same
        # read, so an error has to be sent in answer to `CONNECT` rather than right after `INFO`.
        connection.sendall(self.info_line())

        try:
            self.read_commands(connection)
        except OSError as error:
            log("connection is gone: {}".format(error))
        finally:
            with self.lock:
                self.clients.pop(connection, None)
            connection.close()

    def read_commands(self, connection):
        buffer = b""
        while True:
            data = connection.recv(4096)
            if not data:
                return
            buffer += data

            while b"\r\n" in buffer:
                line, rest = buffer.split(b"\r\n", 1)
                command = line.split()
                name = command[0].upper() if command else b""

                if name in (b"PUB", b"HPUB"):
                    # The payload follows the command line and is not a command itself, so it has
                    # to be consumed here or the loop would try to interpret it as one.
                    payload_size = int(command[-1]) + len(b"\r\n")
                    if len(rest) < payload_size:
                        break
                    rest = rest[payload_size:]
                elif name == b"CONNECT":
                    if self.state() == STATE_REJECT:
                        log("rejecting the credentials of a connection")
                        connection.sendall(AUTHORIZATION_ERR)
                        return
                    log("credentials accepted")
                    with self.lock:
                        self.clients[connection] = {}
                elif name == b"SUB":
                    # `SUB <subject> [queue group] <sid>`
                    subject = command[1].decode()
                    sid = command[-1].decode()
                    with self.lock:
                        if connection in self.clients:
                            self.clients[connection][sid] = subject
                    log("subscribed sid {} to subject {}".format(sid, subject))
                elif name == b"UNSUB":
                    sid = command[1].decode()
                    with self.lock:
                        if connection in self.clients:
                            self.clients[connection].pop(sid, None)
                    log("unsubscribed sid {}".format(sid))
                elif name == b"PING":
                    connection.sendall(PONG)

                buffer = rest

    def enforce_state(self):
        # A connection accepted just before the state changed has to be dropped too, otherwise
        # the client would keep consuming through it and never see the rejection.
        while True:
            if self.state() == STATE_REJECT:
                with self.lock:
                    connections = list(self.clients)
                    self.clients.clear()
                for connection in connections:
                    log("dropping a live connection")
                    try:
                        connection.shutdown(socket.SHUT_RDWR)
                    except OSError as error:
                        log("cannot drop a connection: {}".format(error))
            time.sleep(0.1)

    def publish_loop(self):
        while True:
            time.sleep(self.publish_interval)
            if self.state() != STATE_ACCEPT:
                continue

            with self.lock:
                deliveries = [
                    (connection, sid, subject)
                    for connection, subscriptions in self.clients.items()
                    for sid, subject in subscriptions.items()
                ]

            for connection, sid, subject in deliveries:
                self.published += 1
                payload = json.dumps(
                    {"key": self.published, "value": self.published}
                ).encode()
                message = "MSG {} {} {}\r\n".format(subject, sid, len(payload)).encode()
                try:
                    connection.sendall(message + payload + b"\r\n")
                except OSError as error:
                    log("cannot deliver a message: {}".format(error))


if __name__ == "__main__":
    fake_broker = FakeBroker(
        port=int(sys.argv[1]),
        state_path=sys.argv[2],
        publish_interval=float(sys.argv[3]) if len(sys.argv) > 3 else 0.2,
    )
    fake_broker.run()
