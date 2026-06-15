import http.server
import json
import sys

RESULT_PATH = "/echo_server_headers.txt"
METHOD_PATH = "/echo_server_method.txt"

class RequestHandler(http.server.BaseHTTPRequestHandler):
    def log_body(self, results):
        with open(RESULT_PATH, "w") as f:
            f.write(results)

    def log_method(self, method):
        with open(METHOD_PATH, "w") as f:
            f.write(method)

    def do_GET(self): # for health-check in docker startup script
        self.log_method("GET")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')



    def do_POST(self):
        self.log_method("POST")
        transfer_encoding = self.headers.get('Transfer-Encoding')
        if transfer_encoding == 'chunked':
            body, body_length = self.read_chunked()
        else:
            content_length = int(self.headers.get('Content-Length', 0))
            body, body_length = self.rfile.read(content_length).decode('utf-8', errors='replace'), content_length

        self.log_body(body)
        print("-" * 60)
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')
        return

    def read_chunked(self):
        body = []
        read_bytes = 0
        while True:
            chunk_size_line = self.rfile.readline().strip()
            if not chunk_size_line:
                break
            chunk_size = int(chunk_size_line, 16)
            if chunk_size == 0:
                break
            chunk_data = self.rfile.read(chunk_size)
            read_bytes += len(chunk_data)
            body.append(chunk_data.decode('utf-8', errors='replace'))
            self.rfile.readline()
        return ''.join(body), read_bytes
    


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
