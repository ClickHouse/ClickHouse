import http.server
import sys

RESULT_PATH = "/headers.txt"


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        if self.path == "/dict-data":
            with open(RESULT_PATH, "w") as f:
                f.write(self.headers.as_string())
            self.send_response(200)
            self.send_header("Content-Type", "text/tab-separated-values")
            self.end_headers()
            self.wfile.write(b"1\tfirst\n2\tsecond\n")


if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    httpd = http.server.ThreadingHTTPServer(
        (
            host,
            port,
        ),
        RequestHandler,
    )

    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
