import http.client
import http.server
import random
import socketserver
import sys
import urllib.parse

UPSTREAM_HOST = "minio1:9001"
random.seed("No list objects/1.0")

list_request_counter = 0
list_request_max_number = 10


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


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        query = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(query, keep_blank_values=True)

        global list_request_counter
        global list_request_max_number

        if "list-type" in params:
            if list_request_counter > list_request_max_number:
                self.send_response(501)
                self.send_header("Content-Type", "application/xml")
                self.end_headers()
                self.wfile.write(
                    b"""<?xml version="1.0" encoding="UTF-8"?>
                        <Error>
                            <Code>NotImplemented</Code>
                            <Message>I can list object only once</Message>
                            <Resource>RESOURCE</Resource>
                            <RequestId>REQUEST_ID</RequestId>
                        </Error>
                    """
                )
            else:
                list_request_counter += 1
                self.do_HEAD()
        else:
            self.do_HEAD()

    def do_PUT(self):
        self.do_HEAD()

    def do_DELETE(self):
        self.do_HEAD()

    def do_POST(self):
        global list_request_counter
        global list_request_max_number

        if self.path.startswith("/reset_counters"):
            query = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(query, keep_blank_values=True)

            list_request_counter = 0
            if "max" in params:
                list_request_max_number = int(params["max"][0])

            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        self.do_HEAD()

    def do_HEAD(self):
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


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Handle requests in a separate thread."""


httpd = ThreadedHTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
