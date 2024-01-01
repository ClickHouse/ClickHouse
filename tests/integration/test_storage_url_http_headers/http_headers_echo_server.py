import http.server

RESULT_PATH = "/headers.txt"


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        with open(RESULT_PATH, "w") as f:
            f.write(self.headers.as_string())

    def do_POST(self):
        self.rfile.read1()
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')


if __name__ == "__main__":
    with open(RESULT_PATH, "w") as f:
        f.write("")
    httpd = http.server.HTTPServer(
        (
            "localhost",
            8000,
        ),
        RequestHandler,
    )
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
