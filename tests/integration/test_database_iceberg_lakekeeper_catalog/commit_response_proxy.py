"""Inject snapshot-commit failures before or after forwarding to the catalog."""

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
        self.readback_mode = None
        self.commit_path = None
        self.attempts = 0
        self.statuses = []
        self.reads = 0
        self.intercepted = threading.Event()
        self.committed = threading.Event()
        self.release = threading.Event()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def reply(self, status, body, content_type="application/json"):
        if self.command == "POST" and self.path == self.server.commit_path:
            self.server.statuses.append(status)
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/control/ready":
            self.reply(200, b"ready", "text/plain")
        elif self.path == "/control/counts":
            self.reply(
                200,
                json.dumps({
                    "attempts": self.server.attempts,
                    "statuses": self.server.statuses,
                    "reads": self.server.reads,
                }).encode(),
            )
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
            self.server.readback_mode = command.get("readback_mode", "normal")
            self.server.commit_path = None
            self.server.snapshot = None
            self.server.attempts = 0
            self.server.statuses = []
            self.server.reads = 0
            self.server.intercepted.clear()
            self.server.committed.clear()
            self.server.release.clear()
            self.reply(200, b"{}")
        elif self.path == "/control/release":
            self.server.release.set()
            self.reply(200, b"{}")
        elif self.path == "/control/reset":
            self.server.commit_path = None
            self.reply(200, b"{}")
        else:
            self.forward()

    def read_body(self):
        return self.rfile.read(int(self.headers.get("Content-Length", "0")))

    def forward(self):
        body = self.read_body() if self.command == "POST" else None
        readback = (
            self.command == "GET"
            and self.path == self.server.commit_path
            and self.server.release.is_set()
        )
        if readback:
            self.server.reads += 1
        if readback and (
            self.server.readback_mode == "read_error"
            or (self.server.readback_mode == "read_error_once" and self.server.reads == 1)
        ):
            self.reply(
                403,
                b'{"error":{"message":"Injected read-back failure","type":"ForbiddenException","code":403}}',
            )
            return
        snapshot = None
        intercept = False
        if body:
            request = json.loads(body)
            intercept = self.server.target is not None and request.get("identifier") == self.server.target
            if intercept or self.path == self.server.commit_path:
                snapshot = next(
                    (
                        update["snapshot"]
                        for update in request.get("updates", [])
                        if update.get("action") == "add-snapshot"
                    ),
                    None,
                )
                if snapshot is not None:
                    self.server.commit_path = self.path
                    self.server.attempts += 1

        if (
            snapshot is not None
            and not intercept
            and self.server.response_mode in {"replay_reject", "replay_unavailable"}
        ):
            status = 400 if self.server.response_mode == "replay_reject" else 503
            self.reply(
                status,
                json.dumps({
                    "error": {
                        "message": "Injected replay failure",
                        "type": "BadRequestException" if status == 400 else "ServiceUnavailableException",
                        "code": status,
                    }
                }).encode(),
            )
            return

        if snapshot is not None and intercept and self.server.response_mode in {"reject", "unavailable"}:
            self.server.target = None
            self.server.snapshot = snapshot
            self.server.intercepted.set()
            self.server.release.wait(timeout=120)
            if self.server.response_mode == "unavailable":
                self.reply(
                    503,
                    b'{"error":{"message":"Injected error before commit","type":"ServiceUnavailableException","code":503}}',
                )
            else:
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

        if snapshot is not None and intercept and 200 <= status < 300:
            self.server.target = None
            self.server.snapshot = snapshot
            self.server.commit_path = self.path
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

        if readback and 200 <= status < 300 and self.server.readback_mode in {"missing", "mismatch"}:
            table = json.loads(result)
            snapshots = table["metadata"]["snapshots"]
            snapshot_id = self.server.snapshot["snapshot-id"]
            if self.server.readback_mode == "missing":
                table["metadata"]["snapshots"] = [
                    snapshot for snapshot in snapshots if snapshot["snapshot-id"] != snapshot_id
                ]
            else:
                for snapshot in snapshots:
                    if snapshot["snapshot-id"] == snapshot_id:
                        snapshot["manifest-list"] += ".other"
            result = json.dumps(table).encode()

        self.reply(status, result, content_type)


if __name__ == "__main__":
    Proxy(int(sys.argv[1])).serve_forever()
