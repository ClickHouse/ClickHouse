"""
Mock S3 server that introduces a configurable delay on object listing requests.

Used for testing that KILL QUERY works while the cluster task iterator is
blocked waiting for S3 listings — regression test for GitHub issue #98165.

Endpoints:
  GET /                        → "OK" (health/ping)
  GET /<bucket>?list-type=2&.. → delays LISTING_DELAY_SECONDS, then empty list
  PUT /mock_control/delay/<n>  → change the listing delay to n seconds
"""

import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlparse

# Default delay for listing requests (in seconds).
_delay_lock = threading.Lock()
_listing_delay = 10.0


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        # Health-check / ping endpoint required by start_mock_servers().
        if parsed.path == "/":
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # S3 ListObjectsV2 request.
        if "list-type" in params:
            with _delay_lock:
                delay = _listing_delay
            time.sleep(delay)

            body = (
                b'<?xml version="1.0" encoding="UTF-8"?>'
                b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                b"<Name>test</Name>"
                b"<Prefix></Prefix>"
                b"<KeyCount>0</KeyCount>"
                b"<MaxKeys>1000</MaxKeys>"
                b"<IsTruncated>false</IsTruncated>"
                b"</ListBucketResult>"
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # Any other GET (e.g., object reads): return empty 200.
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_PUT(self):
        parsed = urlparse(self.path)
        parts = parsed.path.strip("/").split("/")
        # PUT /mock_control/delay/<seconds>
        if len(parts) == 3 and parts[0] == "mock_control" and parts[1] == "delay":
            try:
                new_delay = float(parts[2])
            except ValueError:
                self.send_response(400)
                self.end_headers()
                return
            global _listing_delay
            with _delay_lock:
                _listing_delay = new_delay
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, fmt, *args):  # silence request logs
        pass


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle each incoming request in a separate thread."""
    daemon_threads = True


if __name__ == "__main__":
    port = int(sys.argv[1])
    server = ThreadedHTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()
