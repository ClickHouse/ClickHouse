#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a local HTTP server serving index pages.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The carrier that 04673 cannot reach: `CREATE TABLE ... AS url(...)` without a column list, whose
# columns come from schema inference through index-page expansion, so creation needs a listable
# host. Two `clickhouse local` invocations against the same --path: the second reloads the metadata
# the first persisted, which is the path a server takes at startup. Before the fix that reload
# rebuilt the table as `StorageObjectStorage`, re-ran the experimental check and refused to start.

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

# An index page listing two parts, which is all index-page expansion needs to discover them.
python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler

PARTS = {'/data/part1.tsv': b'1\n', '/data/part2.tsv': b'2\n'}
INDEX = b'<a href=\"part1.tsv\">part1.tsv</a>\n<a href=\"part2.tsv\">part2.tsv</a>\n'

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def body(self):
        path = self.path.split('?')[0]
        if path == '/data/':
            return INDEX, 'text/html'
        return PARTS.get(path), 'text/plain'

    def respond(self, with_body):
        data, content_type = self.body()
        if data is None:
            self.send_response(404)
            self.end_headers()
            if with_body:
                self.wfile.write(b'Not Found')
            return
        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        if with_body:
            self.wfile.write(data)

    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond(True)

HTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/data/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

WORKING_FOLDER="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

GLOB_URL="http://127.0.0.1:${HTTP_PORT}/data/*.tsv"

NO_RETRY="SET http_max_tries = 1;"

# Prints the rows an arm asked for, then whether the experimental gate refused anything. Counting
# the gate rather than echoing the error keeps the reference free of connection wording.
run_local() {
    local out
    out=$(${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "${NO_RETRY} $1" \
        -- --max_server_memory_usage=10G --memory_worker_use_cgroup=0 2>&1)
    echo "$out" | grep -E '^(adhoc|created|column|reloaded)\b' || true
    echo -n 'gate refused: '
    echo "$out" | grep -c 'SUPPORT_IS_DISABLED' || true
}

# Reading is what builds the storage, so this is where a reload consults the gate: `clickhouse
# local` resolves a table lazily, while a server builds it while loading metadata and refuses to
# start when the gate throws. The reloaded table reads the literal wildcard URL, which the server
# answers with 404, so `read ok` is 0 and the gate count next to it is what the fix moves.
read_outcome() {
    local out
    if out=$(${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "${NO_RETRY} $1 SELECT count() FROM d.t;" \
        -- --max_server_memory_usage=10G --memory_worker_use_cgroup=0 2>&1)
    then echo 'read ok: 1'
    else echo 'read ok: 0'
    fi
    echo -n 'gate refused: '
    echo "$out" | grep -c 'SUPPORT_IS_DISABLED' || true
}

echo '--- the host is listable, so index-page expansion has something to discover'
run_local "
    SET allow_experimental_url_wildcard_from_index_pages = 1;
    SELECT 'adhoc', count() FROM url('${GLOB_URL}', TSV);"

echo '--- created with the setting on, columns inferred through index-page expansion'
run_local "
    SET allow_experimental_url_wildcard_from_index_pages = 1;
    CREATE DATABASE d;
    CREATE TABLE d.t AS url('${GLOB_URL}', TSV);
    SELECT 'created', engine FROM system.tables WHERE database = 'd' AND name = 't';
    SELECT 'column', name, type FROM system.columns WHERE database = 'd' AND table = 't';"

echo '--- reloaded with the setting off'
# The reload reports the column list the creation stored: inference is baked into the `ATTACH` it
# wrote, so nothing is inferred again here.
run_local "
    SELECT 'reloaded', engine FROM system.tables WHERE database = 'd' AND name = 't';
    SELECT 'column', name, type FROM system.columns WHERE database = 'd' AND table = 't';"
read_outcome ""

echo '--- reloaded with the setting on'
# Metadata replay resolves the definition under the loading context, not the session, so turning
# the setting back on changes nothing: before the fix the persisted table was unusable either way,
# and a server never got as far as a session because loading its metadata aborted startup.
read_outcome "SET allow_experimental_url_wildcard_from_index_pages = 1;"

rm -rf "${WORKING_FOLDER}"
