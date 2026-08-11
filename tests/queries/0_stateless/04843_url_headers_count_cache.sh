#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A read of `_headers` from `url()` must not take the cached-row-count shortcut:
# that branch never performs the HTTP request, so the virtual column would be an
# empty map instead of the actual response headers.

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

# A tiny HTTP server that serves a static payload with a `Last-Modified` header
# and a recognizable custom header.
#
# Note that the queries below must also set `schema_inference_cache_require_modification_time_for_url = 0`:
# for a single URL option the read buffer is initialized lazily, so at the moment the row-count cache is
# probed no response has been received yet and the last modification time is unknown, which would make the
# cache never engage and the assertion below vacuous.
python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler

PAYLOAD = b'a\nb\nc\n'

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/tab-separated-values')
        self.send_header('Content-Length', str(len(PAYLOAD)))
        self.send_header('Last-Modified', 'Tue, 11 Aug 2026 00:00:00 GMT')
        self.send_header('X-Test-Header', 'present')
        self.end_headers()
        self.wfile.write(PAYLOAD)
    def log_message(self, *args):
        pass

HTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!
trap 'kill ${HTTP_PID} 2>/dev/null; wait ${HTTP_PID} 2>/dev/null' EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/data.tsv" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

URL="http://127.0.0.1:${HTTP_PORT}/data.tsv"

# Populate the row-count cache with a plain count.
${CLICKHOUSE_CLIENT} -q "
SELECT count() FROM url('${URL}', TSV, 's String')
SETTINGS use_cache_for_count_from_files = 1, schema_inference_cache_require_modification_time_for_url = 0;
"

# Sanity check that the cache actually engages for this URL: a repeated count
# must hit the num-rows cache (otherwise the assertion below would be vacuous).
${CLICKHOUSE_CLIENT} -q "
SELECT count() FROM url('${URL}', TSV, 's String')
SETTINGS use_cache_for_count_from_files = 1, schema_inference_cache_require_modification_time_for_url = 0, log_comment = '04843_repeat_count';
"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
SELECT ProfileEvents['SchemaInferenceCacheNumRowsHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04843_repeat_count' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;
"

# `_headers` must contain the real response headers even though the row count
# for this URL is cached and no data columns are requested.
${CLICKHOUSE_CLIENT} -q "
SELECT DISTINCT _headers['X-Test-Header'] FROM url('${URL}', TSV, 's String')
SETTINGS use_cache_for_count_from_files = 1, schema_inference_cache_require_modification_time_for_url = 0;
"
