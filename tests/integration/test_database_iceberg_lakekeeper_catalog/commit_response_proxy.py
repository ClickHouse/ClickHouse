"""Inject a snapshot-commit rejection or lose one successful commit response."""

import json
import socket
import sys
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Proxy(ThreadingHTTPServer):
    def __init__(self, port):
        super().__init__(("0.0.0.0", port), Handler)
        self.target = None
        self.snapshot = None
        self.response_mode = None
        self.intercepted = threading.Event()
        self.committed = threading.Event()
        self.release = threading.Event()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def reply(self, status, body, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/control/ready":
            self.reply(200, b"ready", "text/plain")
        elif self.path in {"/control/committed", "/control/intercepted"}:
            event = self.server.committed if self.path == "/control/committed" else self.server.intercepted
            if not event.wait(timeout=60):
                self.reply(504, b'{"error":"commit did not reach the expected stage"}')
                return
            self.reply(200, json.dumps(self.server.snapshot).encode())
        else:
            self.forward()

    def do_POST(self):
        if self.path == "/control/arm":
            command = json.loads(self.read_body())
            self.server.target = command["identifier"]
            self.server.response_mode = command["response_mode"]
            self.server.snapshot = None
            self.server.intercepted.clear()
            self.server.committed.clear()
            self.server.release.clear()
            self.reply(200, b"{}")
        elif self.path == "/control/release":
            self.server.release.set()
            self.reply(200, b"{}")
        else:
            self.forward()

    def read_body(self):
        return self.rfile.read(int(self.headers.get("Content-Length", "0")))

    def forward(self):
        body = self.read_body() if self.command == "POST" else None
        snapshot = None
        if body and self.server.target is not None:
            request = json.loads(body)
            if request.get("identifier") == self.server.target:
                snapshot = next(
                    (
                        update["snapshot"]
                        for update in request.get("updates", [])
                        if update.get("action") == "add-snapshot"
                    ),
                    None,
                )

        if snapshot is not None and self.server.response_mode == "reject":
            self.server.target = None
            self.server.snapshot = snapshot
            self.server.intercepted.set()
            self.server.release.wait(timeout=120)
            self.reply(
                400,
                b'{"error":{"message":"Injected commit rejection","type":"BadRequestException","code":400}}',
            )
            return

        headers = {
            name: value
            for name, value in self.headers.items()
            if name.lower() not in {"host", "connection", "transfer-encoding"}
        }
        upstream = urllib.request.Request(
            "http://lakekeeper:8181" + self.path,
            data=body,
            headers=headers,
            method=self.command,
        )
        try:
            response = urllib.request.urlopen(upstream, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            result = response.read()
            status = response.code
            content_type = response.headers.get("Content-Type", "application/json")

        if snapshot is not None and 200 <= status < 300:
            self.server.target = None
            self.server.snapshot = snapshot
            self.server.intercepted.set()
            self.server.committed.set()
            # Keep the successful response pending until the test has read the snapshot.
            self.server.release.wait(timeout=120)
            if self.server.response_mode == "server_error":
                self.reply(
                    503,
                    b'{"error":{"message":"Injected error after commit","type":"ServiceUnavailableException","code":503}}',
                )
                return
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return

        self.reply(status, result, content_type)


if __name__ == "__main__":
    Proxy(int(sys.argv[1])).serve_forever()
