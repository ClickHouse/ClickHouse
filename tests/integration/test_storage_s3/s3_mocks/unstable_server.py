import http.server
import random
import re
import sys
import threading
import time


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
    b"".join([gen_line() for _ in range(500000)])
    + f"0,0,0,{-sum_in_4_column}\n".encode()
)

cancel_test_condition = threading.Condition()
# State controlled through `/cancel_test/*`. ClickHouse reads the simulated S3 object through the
# `resolver:8081` Docker address, while the test invokes this private API as `localhost:8081` from
# inside the resolver container.
cancel_test_requests = 0
cancel_test_release = False


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def send_text(self, text):
        data = text.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def handle_cancel_test_control(self):
        """Reset, inspect, or release the response used by the cancellation test."""
        global cancel_test_requests
        global cancel_test_release

        if self.path == "/cancel_test/reset":
            with cancel_test_condition:
                cancel_test_requests = 0
                cancel_test_release = False
            self.send_text("OK")
            return True

        if self.path == "/cancel_test/status":
            with cancel_test_condition:
                requests = cancel_test_requests
            self.send_text(str(requests))
            return True

        if self.path == "/cancel_test/release":
            with cancel_test_condition:
                cancel_test_release = True
                cancel_test_condition.notify_all()
            self.send_text("OK")
            return True

        return False

    def do_HEAD(self):
        if self.path in (
            "/root/test.csv",
            "/root/slow_send_test.csv",
            "/root/cancel_during_retry.csv",
        ):
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
        global cancel_test_requests

        if self.handle_cancel_test_control():
            return

        self.do_HEAD()
        if self.path == "/root/cancel_during_retry.csv":
            with cancel_test_condition:
                cancel_test_requests += 1

            # Send a prefix of the requested range, then wait for the test to decide exactly when
            # reading the response stream should fail. Keep it at least one byte shorter than the
            # advertised `Content-Length`, including for short ranged requests.
            requested_length = self.end_bytes - self.from_bytes
            prefix_length = min(1024 * 1024, max(0, requested_length - 1))
            self.wfile.write(
                lines[self.from_bytes : self.from_bytes + prefix_length]
            )
            self.wfile.flush()

            with cancel_test_condition:
                cancel_test_condition.wait_for(lambda: cancel_test_release)

            # Return with fewer bytes than promised by `Content-Length`; the test pauses
            # `processException` after this stream failure and cancels the query there.
            return

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
                    self.log_message("Dropping connection %s", self.path)
                    break

        if self.path == "/root/slow_send_test.csv":
            # Stream the whole dataset with a 1s stall after each block so the
            # slow-GET path is still exercised. A 1 MiB block keeps the number
            # of stalls (~17 for the full file) low enough to bound test time.
            self.send_block_size = 1024 * 1024

            for c, i in enumerate(
                range(self.from_bytes, self.end_bytes, self.send_block_size)
            ):
                self.wfile.write(
                    lines[i : min(i + self.send_block_size, self.end_bytes)]
                )
                self.wfile.flush()
                time.sleep(1)

        elif self.path == "/":
            self.wfile.write(b"OK")


httpd = http.server.ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
