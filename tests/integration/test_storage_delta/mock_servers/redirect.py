import http.server
import sys
import threading

hits = 0
hits_lock = threading.Lock()


class TargetHandler(http.server.BaseHTTPRequestHandler):
    def handle_any(self):
        global hits
        with hits_lock:
            hits += 1
        self.send_error(500)

    do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = handle_any

    def log_message(self, *args):
        pass


class RedirectHandler(http.server.BaseHTTPRequestHandler):
    def handle_any(self):
        if self.path == "/":
            body = b"OK"
        elif self.path == "/hits":
            with hits_lock:
                body = str(hits).encode()
        else:
            self.send_response(302)
            self.send_header("Location", f"http://resolver:{target_port}/redirected")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = handle_any

    def log_message(self, *args):
        pass


port = int(sys.argv[1])
target_port = port + 1
target = http.server.ThreadingHTTPServer(("0.0.0.0", target_port), TargetHandler)
threading.Thread(target=target.serve_forever, daemon=True).start()
http.server.ThreadingHTTPServer(("0.0.0.0", port), RedirectHandler).serve_forever()
