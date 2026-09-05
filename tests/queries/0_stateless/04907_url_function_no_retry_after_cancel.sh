#!/usr/bin/env bash
# Tags: no-fasttest
# A schema-inference read has no `StorageURLSource` cancellation token. Killing it while its HTTP
# retry backoff is pending must still stop it promptly, without waiting for the full backoff.

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
    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-Length', '0')
        self.end_headers()
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

QUERY_ID="${CLICKHOUSE_DATABASE}_schema_inference_cancel"
$CLICKHOUSE_CLIENT \
    --parallel_replicas_for_cluster_engines 0 \
    --query_id "$QUERY_ID" \
    --query "SELECT * FROM url('http://127.0.0.1:$HTTP_PORT/failed', 'CSV') SETTINGS http_max_tries = 2, http_retry_initial_backoff_ms = 60000, http_retry_max_backoff_ms = 120000" \
    >/dev/null 2>/dev/null &
CLIENT_PID=$!

for _ in {1..300}; do
    if [[ $(curl -sS "http://127.0.0.1:$HTTP_PORT/stats" | python3 -c "import sys, json; print(json.load(sys.stdin).get('GET', 0))") == 1 ]]; then
        break
    fi
    sleep 0.1
done

# `EPOCHREALTIME` is `seconds.microseconds`; stripping the dot makes it microseconds.
START_US=${EPOCHREALTIME/./}
# `KILL QUERY ... SYNC` reports the killed queries in its result, which is not what this test checks.
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC FORMAT Null"
wait $CLIENT_PID
CLIENT_STATUS=$?
ELAPSED_MS=$(( (${EPOCHREALTIME/./} - START_US) / 1000 ))
GET_COUNT=$(curl -sS "http://127.0.0.1:$HTTP_PORT/stats" | python3 -c "import sys, json; print(json.load(sys.stdin).get('GET', 0))")

if ((CLIENT_STATUS != 0)); then
    echo "the cancelled schema-inference query failed"
else
    echo "FAIL: the cancelled schema-inference query succeeded"
fi

# The measured interval includes starting the `KILL QUERY` client and tearing the killed client
# down, which alone takes seconds under the sanitizer builds - hence the wide margin against the
# 60-second backoff.
if ((ELAPSED_MS < 15000)); then
    echo "the cancellation interrupted the retry backoff"
else
    echo "FAIL: the cancellation waited $ELAPSED_MS ms for the retry backoff"
fi

if ((GET_COUNT == 1)); then
    echo "no retry request was started after cancellation"
else
    echo "FAIL: expected one request before cancellation, got $GET_COUNT"
fi
