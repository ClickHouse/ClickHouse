import http.server
import sys

REDIRECT_HOST = ""
REDIRECT_PORT = 0

RESULT_PATH = "/redirect_server_headers.txt"


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        with open(RESULT_PATH, "w") as f:
            f.write(self.headers.as_string())

    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        else:
            global REDIRECT_HOST, REDIRECT_PORT
            self.send_response(302)
            target_location = f"http://{REDIRECT_HOST}:{REDIRECT_PORT}{self.path}"
            self.send_header("Location", target_location)
            self.end_headers()
            self.wfile.write(b'{"status":"redirected"}')


if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    REDIRECT_HOST = sys.argv[3]
    REDIRECT_PORT = int(sys.argv[4])
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
