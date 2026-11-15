import http.client
import http.server
import json
import socketserver
import sys
import threading

# A proxy in front of the moto Glue mock that fails `GetTables` for some databases and checks the
# `VersionId` of `UpdateTable` for others, because moto ignores it.
# `GET /injected/<database>` returns how many faults were injected for that database.
UPSTREAM = ("glue", 3000)

# Database name prefix -> (HTTP status, exception name, message, number of requests to fail or None for all).
FAULTS = {
    "transient_": (500, "InternalServiceException", "Unknown exception occurred.", 2),
    "denied_": (400, "AccessDeniedException", "Injected access denied", None),
}

# Database name prefix whose `UpdateTable` is rejected with `ConcurrentModificationException` on a stale `VersionId`.
VERSION_CHECKED_PREFIX = "cas_"

DROPPED_HEADERS = {"connection", "keep-alive", "transfer-encoding", "content-length", "host", "server", "date"}

injected = {}
lock = threading.Lock()
update_lock = threading.Lock()


def take_fault(database):
    for prefix, (status, name, message, limit) in FAULTS.items():
        if not database.startswith(prefix):
            continue
        with lock:
            count = injected.get(database, 0)
            if limit is not None and count >= limit:
                return None
            injected[database] = count + 1
        return status, name, message
    return None


def count_fault(database):
    with lock:
        injected[database] = injected.get(database, 0) + 1


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def reply(self, status, body, headers):
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/":
            self.reply(200, b"OK", [("Content-Type", "text/plain")])
        elif self.path.startswith("/injected/"):
            with lock:
                count = injected.get(self.path[len("/injected/") :], 0)
            self.reply(200, str(count).encode(), [("Content-Type", "text/plain")])
        else:
            self.forward(None)

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        target = self.headers.get("X-Amz-Target")
        if target == "AWSGlue.GetTables":
            fault = take_fault(json.loads(body).get("DatabaseName", ""))
            if fault is not None:
                status, name, message = fault
                payload = json.dumps({"__type": name, "message": message}).encode()
                self.reply(status, payload, [("Content-Type", "application/x-amz-json-1.1")])
                return
        if target == "AWSGlue.UpdateTable":
            request = json.loads(body)
            database = request.get("DatabaseName", "")
            if database.startswith(VERSION_CHECKED_PREFIX) and "VersionId" in request:
                with update_lock:
                    if self.current_version_id(database, request["TableInput"]["Name"]) != request["VersionId"]:
                        count_fault(database)
                        payload = json.dumps(
                            {"__type": "ConcurrentModificationException", "message": "Stale VersionId"}
                        ).encode()
                        self.reply(400, payload, [("Content-Type", "application/x-amz-json-1.1")])
                        return
                    self.forward(body)
                return
        self.forward(body)

    def forwarded_headers(self):
        return {k: v for k, v in self.headers.items() if k.lower() not in DROPPED_HEADERS}

    def upstream(self, body, headers):
        connection = http.client.HTTPConnection(*UPSTREAM)
        try:
            connection.request(self.command, self.path, body=body, headers=headers)
            response = connection.getresponse()
            return response.status, response.read(), response.getheaders()
        finally:
            connection.close()

    def current_version_id(self, database, table):
        headers = {k: v for k, v in self.forwarded_headers().items() if k.lower() != "x-amz-target"}
        headers["X-Amz-Target"] = "AWSGlue.GetTable"
        _, content, _ = self.upstream(json.dumps({"DatabaseName": database, "Name": table}).encode(), headers)
        return json.loads(content)["Table"]["VersionId"]

    def forward(self, body):
        status, content, headers = self.upstream(body, self.forwarded_headers())
        kept = [(k, v) for k, v in headers if k.lower() not in DROPPED_HEADERS]
        self.reply(status, content, kept)


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


ThreadedHTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler).serve_forever()
