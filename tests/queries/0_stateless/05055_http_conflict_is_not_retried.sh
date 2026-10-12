#!/usr/bin/env bash
# Tags: no-fasttest

# `409 Conflict` reports a state mismatch on the server, so resending the same bytes cannot succeed.
# Retrying it only postpones the error the caller has to act on: an Iceberg REST catalog commit
# refused with `409` was resent `http_max_tries` times with backoff, adding ~32 seconds per conflict.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

requests = 0
lock = threading.Lock()

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        global requests
        if self.path == '/requests':
            with lock:
                body = str(requests).encode()
            self.send_response(200)
        else:
            with lock:
                requests += 1
            body = b'{\"error\": \"conflict\"}'
            self.send_response(409)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass

ThreadingHTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!

trap 'kill $HTTP_PID 2>/dev/null' EXIT

for _ in {1..100}; do
    if [ "$(${CLICKHOUSE_CURL} -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${HTTP_PORT}/requests")" = "200" ]; then
        break
    fi
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://127.0.0.1:${HTTP_PORT}/data', JSONEachRow, 'x Int32')" 2>&1 | grep -o "HTTP status code: 409" | head -n 1

echo "requests: $(${CLICKHOUSE_CURL} -s "http://127.0.0.1:${HTTP_PORT}/requests")"
