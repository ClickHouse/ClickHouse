#!/usr/bin/env bash
# Tags: no-fasttest
# A soft `max_execution_time` with `timeout_overflow_mode = 'break'` is noticed by the executor's
# `checkTimeLimitSoft` polls, which happen between the execution steps. Over the HTTP interface the
# query runs under `CompletedPipelineExecutor`, so with a single-source `url` read there is no
# executor thread left to poll while the source sleeps in its HTTP retry backoff: the wait itself
# must poll the time limit, or the deadline is missed by the remainder of the backoff.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

python3 -u -c "
import json
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

counts = {}
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/stats':
            body = json.dumps(counts).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        counts['GET'] = counts.get('GET', 0) + 1
        self.send_error(503)
    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
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

# `EPOCHREALTIME` is `seconds.microseconds`; stripping the dot makes it microseconds.
START_US=${EPOCHREALTIME/./}
RESULT=$(${CLICKHOUSE_CURL} --max-time 300 -sS "${CLICKHOUSE_URL}&max_execution_time=5&timeout_overflow_mode=break&max_threads=1&parallel_replicas_for_cluster_engines=0&http_max_tries=2&http_make_head_request=0&http_retry_initial_backoff_ms=60000&http_retry_max_backoff_ms=120000" \
    --data-binary "SELECT * FROM url('http://127.0.0.1:$HTTP_PORT/failed', 'CSV', 'x UInt64')" 2>&1)
ELAPSED_MS=$(( (${EPOCHREALTIME/./} - START_US) / 1000 ))
GET_COUNT=$(curl -sS "http://127.0.0.1:$HTTP_PORT/stats" | python3 -c "import sys, json; print(json.load(sys.stdin).get('GET', 0))")

if [[ -z "$RESULT" ]]; then
    echo "the query with a break timeout returned an empty partial result"
else
    echo "FAIL: the query returned: $RESULT"
fi

# The deadline is 5 seconds and the backoff is 60: anything far below the backoff means the
# timeout interrupted the wait. The margin absorbs the query startup under the sanitizer builds.
if ((ELAPSED_MS < 30000)); then
    echo "the timeout interrupted the retry backoff"
else
    echo "FAIL: the timeout waited $ELAPSED_MS ms for the retry backoff"
fi

if ((GET_COUNT < 2)); then
    echo "no retry request was started after the timeout"
else
    echo "FAIL: expected no retry after the timeout, got $GET_COUNT requests"
fi
