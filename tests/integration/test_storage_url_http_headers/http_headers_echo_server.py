import http.server
import json
import sys

RESULT_PATH = "/echo_server_headers.txt"


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
        if self.path == "/sample-data":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            sample_data = [
                {
                    "title": "ClickHouse Newsletter June 2022: Materialized, but still real-time",
                    "theme": "Newsletter",
                },
                {
                    "title": "ClickHouse Over the Years with Benchmarks",
                    "theme": "ClickHouse Journey",
                },
            ]
            self.wfile.write(bytes(json.dumps(sample_data), "UTF-8"))

    def do_POST(self):
        self.rfile.read1()
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')


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
