#!/usr/bin/env bash
# Tags: use_maxminddb

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail
directory="$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME"
collection="maxminddb_$CLICKHOUSE_TEST_UNIQUE_NAME"
mkdir -p "$directory"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS $collection; DROP NAMED COLLECTION IF EXISTS ${collection}_url" >/dev/null
    [[ -z "${origin_pid:-}" ]] || kill "$origin_pid" 2>/dev/null || true
    [[ -z "${origin_pid:-}" ]] || wait "$origin_pid" 2>/dev/null || true
    rm -rf "$directory"
}
trap cleanup EXIT

python3 - "$directory/port" <<'PY' > "$directory/origin.log" 2>&1 &
import pathlib
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

class Handler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-Length', '1024')
        self.send_header('ETag', '"fixture"')
        self.end_headers()

    def do_GET(self):
        body = self.headers.get('Authorization', self.headers.get('Cookie', self.headers.get('X-API-Key', 'public-error-body'))).encode()
        self.send_response(403, body.decode())
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
path = pathlib.Path(sys.argv[1])
path.with_suffix('.next').write_text(str(server.server_address[1]))
path.with_suffix('.next').replace(path)
server.serve_forever()
PY
origin_pid=$!

python3 - "$directory/port" <<'PY'
import pathlib
import sys
import time
deadline = time.monotonic() + 30
while not pathlib.Path(sys.argv[1]).exists():
    if time.monotonic() > deadline:
        raise RuntimeError('HTTP fixture origin did not start')
PY
port=$(cat "$directory/port")

expect_error()
{
    local actual=0
    $CLICKHOUSE_CLIENT --allow_experimental_maxminddb_table_engine 1 --send_logs_level debug -q "$1" > "$directory/error.out" 2> "$directory/error.err" || actual=$?
    [[ "$actual" == 86 ]] && grep -q 'Code: 86\.' "$directory/error.err" || { cat "$directory/error.err" >&2; exit 1; }
}

for header in Authorization Cookie X-API-Key; do
    $CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION $collection AS url='http://127.0.0.1:$port/error', headers.header1.name='$header', headers.header1.value='fixture-reflected-credential'"
    expect_error "CREATE TABLE maxminddb_$CLICKHOUSE_TEST_UNIQUE_NAME (ip IPv4) ENGINE=MaxMindDB($collection) SETTINGS refresh_interval='0'"
    if grep -q 'fixture-reflected-credential' "$directory/error.err"; then
        echo 'Credential leaked in MaxMindDB HTTP diagnostics' >&2
        exit 1
    fi
    echo "$header hidden in MaxMindDB diagnostics"
    $CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${collection}_url AS url='http://127.0.0.1:$port/error', headers.header.name='$header', headers.header.value='fixture-reflected-credential'"
    expect_error "SELECT * FROM url(${collection}_url, format='RawBLOB', structure='data String')"
    if grep -q 'fixture-reflected-credential' "$directory/error.err"; then
        echo 'Credential leaked in URL HTTP diagnostics' >&2
        exit 1
    fi
    echo "$header hidden in URL diagnostics"
    $CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION $collection; DROP NAMED COLLECTION ${collection}_url"
done

expect_error "SELECT * FROM url('http://127.0.0.1:$port/error', 'RawBLOB', 'data String')"
grep -q 'public-error-body' "$directory/error.err"
echo 'Unauthenticated HTTP diagnostics retain the response body'
