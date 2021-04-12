import http.server
import random
import re
import socket
import struct
import sys


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_HEAD(self):
        if self.path == "/root/test.csv":
            self.line = f"{random.randint(10000, 99999)},{random.randint(10000, 99999)},{random.randint(10000, 99999)}\n".encode()
            self.from_bytes = 0
            self.end_bytes = len(self.line)*500000
            self.size = self.end_bytes
            self.stop_at = 1000000
            self.new_at = len(self.line)*10

            if "Range" in self.headers:
                cr = self.headers["Range"]
                parts = re.split("[ -/=]+", cr)
                assert parts[0] == "bytes"
                self.from_bytes = int(parts[1])
                if parts[2]:
                    self.end_bytes = int(parts[2])+1
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {self.from_bytes}-{self.end_bytes-1}/{self.size}")
            else:
                self.send_response(200)

            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", f"{self.end_bytes-self.from_bytes}")
            self.end_headers()

        elif self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()

        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()


    def do_GET(self):
        self.do_HEAD()
        if self.path == "/root/test.csv":
            for c, i in enumerate(range(self.from_bytes, self.end_bytes)):
                self.wfile.write(self.line[i % len(self.line):i % len(self.line) + 1])
                if (c + 1) % self.new_at == 0:
                    self.line = f"{random.randint(10000, 99999)},{random.randint(10000, 99999)},{random.randint(10000, 99999)}\n".encode()
                if (c + 1) % self.stop_at == 0:
                    #self.wfile._sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 0, 0))
                    #self.wfile._sock.shutdown(socket.SHUT_RDWR)
                    #self.wfile._sock.close()
                    print('Dropping connection')
                    break

        elif self.path == "/":
            self.wfile.write(b"OK")


httpd = http.server.HTTPServer(('0.0.0.0', int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
