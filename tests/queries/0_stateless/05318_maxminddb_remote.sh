#!/usr/bin/env bash
# Tags: use_maxminddb, use_aws_s3, use_ssl

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail
directory="$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME"
mkdir -p "$directory"
python3 "$CUR_DIR/helpers/maxminddb.py" "$directory"
echo A > "$directory/state"
python3 "$CUR_DIR/helpers/maxminddb_http.py" "$directory" "$CUR_DIR/../../config/server-cert.pem" "$CUR_DIR/../../config/server-key.pem" > "$directory/ports" 2> "$directory/origin.log" &
origin_pid=$!
table="maxminddb_$CLICKHOUSE_TEST_UNIQUE_NAME"
collection="${table}_source"

query()
{
    $CLICKHOUSE_CLIENT --allow_experimental_maxminddb_table_engine 1 --send_logs_level fatal -q "$1"
}

cleanup()
{
    query "DROP TABLE IF EXISTS $table SYNC; DROP TABLE IF EXISTS ${table}_tls SYNC; DROP TABLE IF EXISTS ${table}_basic SYNC; DROP TABLE IF EXISTS ${table}_token SYNC; DROP TABLE IF EXISTS ${table}_s3 SYNC; DROP TABLE IF EXISTS ${table}_public SYNC; DROP TABLE IF EXISTS ${table}_named SYNC; DROP NAMED COLLECTION IF EXISTS $collection;" >/dev/null
    kill "$origin_pid" 2>/dev/null || true
    wait "$origin_pid" 2>/dev/null || true
    rm -rf "$directory"
}
trap cleanup EXIT

python3 - "$directory/ports" <<'PY'
import json
import pathlib
import sys
import time
path = pathlib.Path(sys.argv[1])
deadline = time.monotonic() + 30
while not path.exists() or not path.stat().st_size:
    if time.monotonic() > deadline:
        raise RuntimeError('HTTP fixture origin did not start')
json.loads(path.read_text())
PY
http_port=$(python3 -c "import json; print(json.load(open('$directory/ports'))['http'])")
https_port=$(python3 -c "import json; print(json.load(open('$directory/ports'))['https'])")
http="http://127.0.0.1:$http_port"
https="https://127.0.0.1:$https_port"

query "CREATE TABLE $table ENGINE=MaxMindDB('$http/public.mmdb') SETTINGS refresh_interval='100ms'"
query "CREATE TABLE ${table}_tls (ip IPv4, version String) ENGINE=MaxMindDB('$https/public.mmdb') SETTINGS refresh_interval='0'"
query "CREATE TABLE ${table}_basic (ip IPv4, version String) ENGINE=MaxMindDB('$http/auth.mmdb', headers('Authorization'='Basic dGVzdDp0ZXN0')) SETTINGS refresh_interval='0'"
query "CREATE NAMED COLLECTION $collection AS url='$http/token.mmdb', headers.header1.name='Authorization', headers.header1.value='Bearer fixture-token'"
query "CREATE TABLE ${table}_token (ip IPv4, version String) ENGINE=MaxMindDB($collection) SETTINGS refresh_interval='0'"
for suffix in '' _tls _basic _token; do
    query "SELECT version FROM ${table}${suffix} WHERE ip=toIPv4('8.8.8.8')"
done
query "SELECT positionCaseInsensitive(create_table_query, 'dGVzdDp0ZXN0')=0 FROM system.tables WHERE database=currentDatabase() AND name='${table}_basic' SETTINGS format_display_secrets_in_show_and_select=0"
status=0
query "CREATE TABLE ${table}_bad (ip IPv4) ENGINE=MaxMindDB('$http/public.mmdb') SETTINGS max_download_size=1" > "$directory/error.out" 2> "$directory/error.err" || status=$?
[[ "$status" == "$((290 % 256))" ]] && grep -q 'Code: 290\.' "$directory/error.err" || { cat "$directory/error.err" >&2; exit 1; }
echo 'Download limit rejected'

wait_origin()
{
    python3 - "$directory/stats" "$1" "${2:-unchanged}" <<'PY'
import json
import pathlib
import sys
import time
path, field, mode = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
before = json.loads(path.read_text())
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    after = json.loads(path.read_text())
    if after[field] >= before[field] + 3:
        if field == 'HEAD' and mode == 'unchanged' and after['GET'] != before['GET']:
            raise RuntimeError('Unchanged MMDB was downloaded during refresh')
        break
else:
    raise RuntimeError('HTTP metadata refresh timed out')
PY
}
wait_origin HEAD
echo 'Unchanged HTTP source uses metadata only'

set_state()
{
    echo "$1" > "$directory/state.next"
    mv "$directory/state.next" "$directory/state"
}

wait_version()
{
    python3 - "$CLICKHOUSE_CLIENT --allow_experimental_maxminddb_table_engine 1" "$1" "$2" <<'PY'
import shlex
import subprocess
import sys
import time
command, table, expected = sys.argv[1:]
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    value = subprocess.check_output(shlex.split(command) + ['-q', f"SELECT version FROM {table} WHERE ip=toIPv4('8.8.8.8')"]).decode().strip()
    if value == expected:
        print('Generation ' + expected)
        break
else:
    raise RuntimeError('Remote MaxMindDB refresh timed out')
PY
}
set_state B
wait_version "$table" B
set_state unavailable
wait_origin failed
query "SELECT version FROM $table WHERE ip=toIPv4('8.8.8.8')"
set_state A
wait_version "$table" A
set_state invalid
wait_origin HEAD changed
query "SELECT version FROM $table WHERE ip=toIPv4('8.8.8.8')"
set_state incompatible
wait_origin HEAD changed
query "SELECT version FROM $table WHERE ip=toIPv4('8.8.8.8')"
set_state A

# Unauthorized probes can disable `HEAD` for the shared HTTP origin, so run them after metadata checks.
if query "CREATE TABLE ${table}_bad (ip IPv4) ENGINE=MaxMindDB('$http/auth.mmdb')" > "$directory/error.out" 2> "$directory/error.err"; then
    echo 'Unauthenticated HTTP request unexpectedly succeeded' >&2
    exit 1
fi
echo 'HTTP authentication rejected'

s3="http://localhost:11111/test/$CLICKHOUSE_TEST_UNIQUE_NAME.mmdb"
upload()
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('$s3', 'test', 'testtest', 'RawBLOB', 'data String') FORMAT RawBLOB" --s3_truncate_on_insert 1 < "$directory/v4_$1.mmdb"
}
upload A
query "CREATE TABLE ${table}_s3 (ip IPv4, version String) ENGINE=MaxMindDB('$s3', 'test', 'testtest') SETTINGS refresh_interval='100ms'"
query "CREATE TABLE ${table}_public (ip IPv4, version String) ENGINE=MaxMindDB('$s3', NOSIGN) SETTINGS refresh_interval='0'"
query "DROP TABLE ${table}_token SYNC; DROP NAMED COLLECTION $collection"
query "CREATE NAMED COLLECTION $collection AS url='$s3', access_key_id='test', secret_access_key='testtest', no_sign_request=false"
query "CREATE TABLE ${table}_named (ip IPv4, version String) ENGINE=MaxMindDB($collection) SETTINGS refresh_interval='0'"
for suffix in _s3 _public _named; do
    query "SELECT version FROM ${table}${suffix} WHERE ip=toIPv4('8.8.8.8')"
done
query "SELECT position(create_table_query, '\\'test\\'')=0 AND position(create_table_query, '\\'testtest\\'')=0 FROM system.tables WHERE database=currentDatabase() AND name='${table}_s3' SETTINGS format_display_secrets_in_show_and_select=0"
for access_key in wrong-key auto; do
    if query "CREATE TABLE ${table}_bad (ip IPv4) ENGINE=MaxMindDB('$s3', '$access_key', 'wrong-secret')" > "$directory/error.out" 2> "$directory/error.err"; then
        echo 'Invalid S3 credentials unexpectedly succeeded' >&2
        exit 1
    fi
done
echo 'S3 authentication rejected'
upload B
wait_version "${table}_s3" B
query "SELECT version FROM ${table}_public WHERE ip=toIPv4('8.8.8.8')"
query "DETACH TABLE ${table}_public; ATTACH TABLE ${table}_public; SELECT version FROM ${table}_public WHERE ip=toIPv4('8.8.8.8')"
