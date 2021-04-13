import http.server
import random
import re
import socket
import struct
import sys


def gen_n_digit_number(n):
    assert 0 < n < 19
    return random.randint(10**(n-1), 10**n-1)

def gen_line():
    # Create fixed length CSV line.
    columns = 4
    total_digits = digits_to_go = (columns-1)*19//2
    columns_to_go = columns-1

    row = []
    def add_number():
        nonlocal digits_to_go
        nonlocal columns_to_go
        min_digits = max(1, digits_to_go - (columns_to_go-1) * 18)
        max_digits = min(18, digits_to_go - (columns_to_go-1))
        digits = random.randint(min_digits, max_digits)
        row.append(gen_n_digit_number(digits))
        columns_to_go -= 1
        digits_to_go -= digits

    for i in range(columns // 2):
        add_number()
    row.append(1)
    for i in range(columns - 1 - columns // 2):
        add_number()

    line = ",".join(map(str, row)) + "\n"
    assert total_digits + 1 == len(line) - columns
    return line.encode()


line = gen_line()
random.seed('Unstable server/1.0')

class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_HEAD(self):
        if self.path == "/root/test.csv":
            self.from_bytes = 0
            self.end_bytes = len(line)*500000
            self.size = self.end_bytes
            self.stop_at = random.randint(900000, 1200000) # Block size is 1024**2.

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
        global line

        self.do_HEAD()
        if self.path == "/root/test.csv":
            lines = 0
            for c, i in enumerate(range(self.from_bytes, self.end_bytes)):
                j = i % len(line)
                self.wfile.write(line[j:j+1])
                if line[j:j+1] == b'\n':
                    line = gen_line()
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
