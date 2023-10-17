import http.server
import random
import re
import socket
import struct
import sys


def gen_n_digit_number(n):
    assert 0 < n < 19
    return random.randint(10 ** (n - 1), 10**n - 1)


sum_in_4_column = 0


def gen_line():
    global sum_in_4_column
    columns = 4

    row = []

    def add_number():
        digits = random.randint(1, 18)
        row.append(gen_n_digit_number(digits))

    for i in range(columns // 2):
        add_number()
    row.append(1)
    for i in range(columns - 1 - columns // 2):
        add_number()
    sum_in_4_column += row[-1]

    line = ",".join(map(str, row)) + "\n"
    return line.encode()


random.seed("Unstable server/1.0")

# Generating some "random" data and append a line which contains sum of numbers in column 4.
lines = (
    b"".join((gen_line() for _ in range(500000)))
    + f"0,0,0,{-sum_in_4_column}\n".encode()
)


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_HEAD(self):
        if self.path == "/root/test.csv":
            self.from_bytes = 0
            self.end_bytes = len(lines)
            self.size = self.end_bytes
            self.send_block_size = 256
            self.stop_at = (
                random.randint(900000, 1300000) // self.send_block_size
            )  # Block size is 1024**2.

            if "Range" in self.headers:
                cr = self.headers["Range"]
                parts = re.split("[ -/=]+", cr)
                assert parts[0] == "bytes"
                self.from_bytes = int(parts[1])
                if parts[2]:
                    self.end_bytes = int(parts[2]) + 1
                self.send_response(206)
                self.send_header(
                    "Content-Range",
                    f"bytes {self.from_bytes}-{self.end_bytes-1}/{self.size}",
                )
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
            for c, i in enumerate(
                range(self.from_bytes, self.end_bytes, self.send_block_size)
            ):
                self.wfile.write(
                    lines[i : min(i + self.send_block_size, self.end_bytes)]
                )
                if (c + 1) % self.stop_at == 0:
                    # self.wfile._sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 0, 0))
                    # self.wfile._sock.shutdown(socket.SHUT_RDWR)
                    # self.wfile._sock.close()
                    print("Dropping connection")
                    break

        elif self.path == "/":
            self.wfile.write(b"OK")


httpd = http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
