#!/usr/bin/env bash
# Tags: use_maxminddb

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail
mkdir -p "$CLICKHOUSE_USER_FILES_UNIQUE"
python3 "$CUR_DIR/helpers/maxminddb.py" "$CLICKHOUSE_USER_FILES_UNIQUE"
table="maxminddb_$CLICKHOUSE_TEST_UNIQUE_NAME"
user="maxminddb_reader_$CLICKHOUSE_TEST_UNIQUE_NAME"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table SYNC; DROP USER IF EXISTS $user" >/dev/null
    [[ -z "${origin_pid:-}" ]] || kill "$origin_pid" 2>/dev/null || true
    [[ -z "${origin_pid:-}" ]] || wait "$origin_pid" 2>/dev/null || true
    rm -rf "$CLICKHOUSE_USER_FILES_UNIQUE"
    rm -f "$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME.port"
}
trap cleanup EXIT

python3 - "$CLICKHOUSE_USER_FILES_UNIQUE" "$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME.port" <<'PY' > "$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME.origin.log" 2>&1 &
import functools
import pathlib
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
server = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(SimpleHTTPRequestHandler, directory=sys.argv[1]))
path = pathlib.Path(sys.argv[2])
path.with_suffix('.next').write_text(str(server.server_address[1]))
path.with_suffix('.next').replace(path)
server.serve_forever()
PY
origin_pid=$!
python3 - "$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME.port" <<'PY'
import pathlib
import sys
import time
deadline = time.monotonic() + 30
while not pathlib.Path(sys.argv[1]).exists():
    if time.monotonic() > deadline:
        raise RuntimeError('HTTP fixture origin did not start')
PY
port=$(cat "$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME.port")

query()
{
    $CLICKHOUSE_CLIENT --user "$user" --allow_experimental_maxminddb_table_engine 1 -q "$1"
}

expect_denied()
{
    local actual=0
    query "$1" > "$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME.out" 2> "$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME.err" || actual=$?
    [[ "$actual" == "$((497 % 256))" ]] && grep -q 'Code: 497\.' "$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME.err" || { cat "$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME.err" >&2; exit 1; }
    [[ -z "${2:-}" ]] || grep -q "$2" "$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME.err"
    echo 'Access denied'
}

$CLICKHOUSE_CLIENT -q "CREATE USER $user; GRANT CREATE TABLE ON $CLICKHOUSE_DATABASE.$table TO $user; GRANT TABLE ENGINE ON MaxMindDB TO $user"
expect_denied "CREATE TABLE $table ENGINE=MaxMindDB('$CLICKHOUSE_TEST_UNIQUE_NAME/v4_A.mmdb') SETTINGS refresh_interval='0'" 'READ ON FILE'
$CLICKHOUSE_CLIENT -q "GRANT READ ON FILE TO $user"
query "CREATE TABLE $table (ip IPv4, version String) ENGINE=MaxMindDB('$CLICKHOUSE_TEST_UNIQUE_NAME/v4_A.mmdb') SETTINGS refresh_interval='0'"
echo 'Local source accepted with READ ON FILE'
expect_denied "SELECT version FROM $table WHERE ip=toIPv4('8.8.8.8')"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(ip, version) ON $CLICKHOUSE_DATABASE.$table TO $user"
query "SELECT version FROM $table WHERE ip=toIPv4('8.8.8.8')"
$CLICKHOUSE_CLIENT -q "DROP TABLE $table SYNC"

# The transport's grant must be checked before contacting an HTTP source.
expect_denied "CREATE TABLE $table (ip IPv4) ENGINE=MaxMindDB('http://127.0.0.1:$port/v4_A.mmdb') SETTINGS refresh_interval='0'" 'READ ON URL'
expect_denied "CREATE TABLE $table (ip IPv4) ENGINE=MaxMindDB('s3://fixture-bucket/missing.mmdb') SETTINGS refresh_interval='0'" 'READ ON S3'
$CLICKHOUSE_CLIENT -q "REVOKE READ ON FILE FROM $user"
uuid=$($CLICKHOUSE_CLIENT -q 'SELECT generateUUIDv4()')
expect_denied "ATTACH TABLE $table UUID '$uuid' (ip IPv4) ENGINE=MaxMindDB('$CLICKHOUSE_TEST_UNIQUE_NAME/v4_A.mmdb') SETTINGS refresh_interval='0'" 'READ ON FILE'
$CLICKHOUSE_CLIENT -q "GRANT READ ON URL TO $user"
query "CREATE TABLE $table (ip IPv4, version String) ENGINE=MaxMindDB('http://127.0.0.1:$port/v4_A.mmdb') SETTINGS refresh_interval='0'"
echo 'HTTP source accepted with READ ON URL without READ ON FILE'
query "SELECT version FROM $table WHERE ip=toIPv4('8.8.8.8')"
rm -f "$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME.out" "$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME.err"
