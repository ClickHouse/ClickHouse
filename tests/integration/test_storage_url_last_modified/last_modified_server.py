import http.server
import sys

BODY = b"1\n"

# RFC 9110, 5.6.7 defines three forms of an `HTTP-date`, and a recipient must accept all of them.
# All three below denote 1994-11-06 08:49:37 UTC, except that the two-digit year of the RFC 850 form
# is resolved to within 50 years of the current time, so 06 means 2006 until 2056-11-06.
LAST_MODIFIED = {
    "/imf-fixdate": "Sun, 06 Nov 1994 08:49:37 GMT",
    "/rfc850": "Monday, 06-Nov-06 08:49:37 GMT",
    "/asctime": "Sun Nov  6 08:49:37 1994",
    "/malformed": "the middle of last week",
    "/missing": None,
}


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
            return

        if self.send_data_headers():
            self.wfile.write(BODY)

    # `url` makes a HEAD request first to learn the file info, and a GET to read the data. Both must
    # report the same `Last-Modified`.
    def do_HEAD(self):
        self.send_data_headers()

    def send_data_headers(self):
        if self.path not in LAST_MODIFIED:
            self.send_response(404)
            self.end_headers()
            return False

        self.send_response(200)
        self.send_header("Content-Type", "text/csv")
        self.send_header("Content-Length", str(len(BODY)))
        last_modified = LAST_MODIFIED[self.path]
        if last_modified is not None:
            self.send_header("Last-Modified", last_modified)
        self.end_headers()
        return True


if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    httpd = http.server.ThreadingHTTPServer((host, port), RequestHandler)

    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
