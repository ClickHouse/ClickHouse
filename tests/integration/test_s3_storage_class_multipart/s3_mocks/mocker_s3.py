import http.client
import http.server
import json
import socketserver
import sys
import urllib.parse

UPSTREAM_HOST = "minio1:9001"

STORAGE_CLASS_HEADER = "x-amz-storage-class"

# Records the storage class header observed for every object-creating request,
# grouped by the kind of S3 operation. Used by the test to verify that
# multipart uploads (CreateMultipartUpload) carry the configured storage class
# and do not fall back to the default STANDARD class.
# See https://github.com/ClickHouse/ClickHouse/issues/68551
recorded = {
    "PutObject": [],
    "CreateMultipartUpload": [],
}


def request(command, url, headers={}, data=None):
    """Mini-requests."""

    class Dummy:
        pass

    parts = urllib.parse.urlparse(url)
    c = http.client.HTTPConnection(parts.hostname, parts.port)
    c.request(
        command,
        urllib.parse.urlunparse(parts._replace(scheme="", netloc="")),
        headers=headers,
        body=data,
    )
    r = c.getresponse()
    result = Dummy()
    result.status_code = r.status
    result.headers = r.headers
    result.content = r.read()
    return result


def classify(command, path):
    """Identify the kind of object-creating S3 operation, or return None."""
    parts = urllib.parse.urlparse(path)
    # `keep_blank_values=True` is required because `CreateMultipartUpload` is sent as
    # `POST /key?uploads` with a valueless `uploads` query parameter.
    query = urllib.parse.parse_qs(parts.query, keep_blank_values=True)
    if command == "POST" and "uploads" in query:
        return "CreateMultipartUpload"
    if command == "PUT" and "partNumber" not in query and "uploadId" not in query:
        return "PutObject"
    return None


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return
        if self.path == "/recorded_storage_classes":
            body = json.dumps(recorded).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.do_HEAD()

    def do_PUT(self):
        self.do_HEAD()

    def do_DELETE(self):
        self.do_HEAD()

    def do_POST(self):
        self.do_HEAD()

    def do_HEAD(self):
        operation = classify(self.command, self.path)
        if operation is not None:
            recorded[operation].append(self.headers.get(STORAGE_CLASS_HEADER))

        content_length = self.headers.get("Content-Length")
        data = self.rfile.read(int(content_length)) if content_length else None
        r = request(
            self.command,
            f"http://{UPSTREAM_HOST}{self.path}",
            headers=self.headers,
            data=data,
        )
        self.send_response(r.status_code)
        for k, v in r.headers.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(r.content)
        self.wfile.close()


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Handle requests in a separate thread."""


httpd = ThreadedHTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
