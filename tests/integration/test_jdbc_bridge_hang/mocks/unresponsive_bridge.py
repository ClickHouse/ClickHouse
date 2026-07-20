import http.server
import sys
import time


# A mock of the JDBC/ODBC bridge that answers the ping handshake but never responds
# to the `columns_info` request, simulating a bridge (or the driver behind it) that
# hangs while structure inference is reading from it.
class RequestHandler(http.server.BaseHTTPRequestHandler):
    def _respond(self, body):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if self.path == "/":
            # Readiness probe used by helpers.mock_servers.start_mock_servers.
            self._respond(b"OK")
        elif self.path.startswith("/ping"):
            # Bridge handshake (IBridgeHelper::PING_OK_ANSWER).
            self._respond(b"Ok.")
        else:
            self._respond(b"Ok.")

    def do_POST(self):
        if self.path.startswith("/columns_info"):
            # Accept the connection but never produce a response, so the structure
            # inference request blocks until its receive timeout fires.
            time.sleep(600)
        else:
            self._respond(b"")

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1])
    # Threading so the hung POST handler does not block ping / readiness requests.
    httpd = http.server.ThreadingHTTPServer(("0.0.0.0", port), RequestHandler)
    httpd.serve_forever()
