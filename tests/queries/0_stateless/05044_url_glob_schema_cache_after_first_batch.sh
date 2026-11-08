#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The lazy glob expansion feeds schema inference in batches of 1000 addresses, and every batch must
# get the schema-cache pass the first one gets. Address 0 through 999 answer with an empty body and
# are skipped; the schema of address 1200 is put into the cache up front, so inference over the
# pattern has to find it there once the second batch appears, instead of reading on. The cache hit
# is what distinguishes the outcomes - the inferred schema would come out the same - so it is what
# the test looks for.

# A local mock serves the addresses: walking the 1000 empty ones of the first batch has to be
# cheap, which 1000 queries against the ClickHouse server itself are not. It answers with one row
# of `0` when the address number is at least 1000 and with an empty body below that.
PORT_FILE="${CLICKHOUSE_TMP}/05044_port_$$"
rm -f "$PORT_FILE"
python3 -u - "$PORT_FILE" <<'EOF' &
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

class Handler(BaseHTTPRequestHandler):
    def respond(self, with_body):
        n = int(self.path.strip("/"))
        body = b"0\n" if n >= 1000 else b""
        self.send_response(200)
        self.send_header("Content-Type", "text/tab-separated-values")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if with_body:
            self.wfile.write(body)

    def do_GET(self):
        self.respond(True)

    def do_HEAD(self):
        self.respond(False)

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
with open(sys.argv[1], "w") as f:
    print(server.server_address[1], file=f)
server.serve_forever()
EOF
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null' EXIT

for _ in {1..300}; do [ -s "$PORT_FILE" ] && break; sleep 0.1; done
PORT=$(cat "$PORT_FILE")
rm -f "$PORT_FILE"

echo "--- the schema of the address after the first batch goes into the cache"
$CLICKHOUSE_CLIENT --query "DESC url('http://127.0.0.1:$PORT/1200', 'TSV')"

echo "--- inference over the pattern takes it from there"
$CLICKHOUSE_CLIENT --query "DESC url('http://127.0.0.1:$PORT/{0..1499}', 'TSV') SETTINGS glob_expansion_max_elements = 2000, engine_url_skip_empty_files = 1, schema_inference_cache_require_modification_time_for_url = 0, log_comment = '05044_url_glob_schema_cache'"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

echo "--- and the query log shows the cache hit"
$CLICKHOUSE_CLIENT --query "SELECT max(ProfileEvents['SchemaInferenceCacheHits']) > 0 FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '05044_url_glob_schema_cache' AND type = 'QueryFinish'"
