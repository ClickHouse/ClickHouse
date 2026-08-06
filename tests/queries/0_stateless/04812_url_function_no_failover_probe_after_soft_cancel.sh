#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers-lsan
# Test that a cancellation which lands between the failover options of the `url` function stops
# `StorageURLSource::getFirstAvailableURIAndReadBuffer` before it starts the next option. A soft
# `max_execution_time` timeout with the `break` overflow mode does not kill the query, so it does
# not interrupt any request: when it arrives while an already skipped empty file is being served,
# there is no in-flight request to wake up, and only an explicit check of the cancellation flag
# between the options keeps the next one from being probed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A server which counts the requests to each path, so that the test can assert which requests were
# made after the cancellation. It binds to the port 0 and reports the port the kernel gave it, so
# that it cannot collide with anything else running in parallel.
#
# - /empty_slow serves an empty file after a delay longer than the soft timeout of the query, so
#   that the cancellation is already pending when the empty file is skipped and the next failover
#   option is about to be probed.
# - /empty serves an empty file at once.
# - /data serves the data at once.
python3 -u -c "
import json
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

counts = {}

class Handler(BaseHTTPRequestHandler):
    def respond(self, head):
        method = 'HEAD' if head else 'GET'
        counts[f'{method} {self.path}'] = counts.get(f'{method} {self.path}', 0) + 1
        if self.path == '/health':
            self.send_response(200)
            self.end_headers()
            if not head:
                self.wfile.write(b'OK')
        elif self.path == '/stats':
            body = json.dumps(counts).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            if not head:
                self.wfile.write(body)
        elif self.path in ('/empty', '/empty_slow'):
            if self.path == '/empty_slow':
                time.sleep(3)
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv')
            self.send_header('Content-Length', '0')
            self.end_headers()
        elif self.path == '/data':
            body = b'1\n2\n3\n4\n5\n'
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            if not head:
                self.wfile.write(body)
        else:
            self.send_error(503)

    def do_HEAD(self):
        self.respond(head=True)

    def do_GET(self):
        self.respond(head=False)

    def log_message(self, *args):
        pass

server = HTTPServer(('127.0.0.1', 0), Handler)
with open('$PORT_FILE', 'w') as f:
    f.write(str(server.server_address[1]))
server.serve_forever()
" &
HTTP_PID=$!
trap 'kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE"' EXIT

for _ in {1..300}; do
    [[ -s "$PORT_FILE" ]] && break
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")

for _ in {1..300}; do
    curl -sS "http://127.0.0.1:$HTTP_PORT/health" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

stat_count()
{
    curl -sS "http://127.0.0.1:$HTTP_PORT/stats" | python3 -c "import sys, json; print(json.load(sys.stdin).get('$1', 0))"
}

# 1. The soft timeout of 1 second fires while the empty file is being served for 3 seconds. When
# the empty file is skipped, the query is already cancelled, so the next failover option must not
# be probed - the query succeeds with no rows and /data gets no requests.
ERROR_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.err")
STARTED_AT=$EPOCHSECONDS
RESULT=$($CLICKHOUSE_CLIENT \
    --engine_url_skip_empty_files 1 \
    --max_execution_time 1 \
    --timeout_overflow_mode break \
    --query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/empty_slow|http://127.0.0.1:$HTTP_PORT/data', 'CSV', 'x UInt64')" 2>"$ERROR_FILE")
CLIENT_STATUS=$?
ELAPSED=$((EPOCHSECONDS - STARTED_AT))

if ((CLIENT_STATUS == 0)); then
    echo "the query timed out while the empty file was being served succeeded"
else
    echo "FAIL: the query timed out while the empty file was being served failed: $(cat "$ERROR_FILE")"
fi
rm -f "$ERROR_FILE"

if [[ -z "$RESULT" ]]; then
    echo "no rows were emitted after the cancellation"
else
    echo "FAIL: the cancelled query returned rows: $RESULT"
fi

if ((ELAPSED < 30)); then
    echo "returned soon after the empty file was served"
else
    echo "FAIL: the query kept running for $ELAPSED seconds"
fi

if (($(stat_count "GET /data") + $(stat_count "HEAD /data") == 0)); then
    echo "the next failover option was not probed after the cancellation"
else
    echo "FAIL: the next failover option was probed after the cancellation"
fi

# 2. A query which is not interrupted keeps the old behavior: the empty file is skipped and the
# data of the next failover option is served.
if [[ $($CLICKHOUSE_CLIENT \
    --engine_url_skip_empty_files 1 \
    --query "SELECT count() FROM url('http://127.0.0.1:$HTTP_PORT/empty|http://127.0.0.1:$HTTP_PORT/data', 'CSV', 'x UInt64')") == 5 ]]; then
    echo "a query which is not interrupted skips the empty file and reads the next option"
else
    echo "FAIL: a query which is not interrupted did not read the data"
fi
