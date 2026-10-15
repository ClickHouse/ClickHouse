#!/usr/bin/env bash
# Tags: use_maxminddb, use_libarchive, use_ssl, use_aws_s3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail
directory="$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME"
mkdir -p "$directory" "$CLICKHOUSE_USER_FILES_UNIQUE"
python3 "$CUR_DIR/helpers/maxminddb.py" "$directory"
python3 - "$directory" <<'PY'
import io
import pathlib
import sys
import tarfile

path = pathlib.Path(sys.argv[1])
for state in ('A', 'B', 'empty', 'ambiguous', 'oversized', 'unsafe', 'invalid'):
    with tarfile.open(path / f'{state}.tar.gz', 'w:gz') as archive:
        def add(name, data):
            entry = tarfile.TarInfo(name)
            entry.size = len(data)
            archive.addfile(entry, io.BytesIO(data))
        add('GeoLite2-City_2026-01-01/LICENSE.txt', b'Fixture license')
        if state in ('A', 'B', 'ambiguous'):
            add('GeoLite2-City_2026-01-01/GeoLite2-City.mmdb', (path / f'v4_{state if state != "ambiguous" else "A"}.mmdb').read_bytes())
        if state == 'ambiguous':
            add('second.mmdb', (path / 'v4_A.mmdb').read_bytes())
        if state == 'oversized':
            add('huge.txt', b'x' * 4096)
        if state == 'unsafe':
            add('../escape.mmdb', (path / 'v4_A.mmdb').read_bytes())
        if state == 'invalid':
            add('GeoLite2-City.mmdb', b'Not a database')
corrupt = bytearray((path / 'A.tar.gz').read_bytes())
corrupt[-8] ^= 0xFF
(path / 'corrupt.tar.gz').write_bytes(corrupt)
PY
echo A > "$directory/state"
python3 "$CUR_DIR/helpers/maxminddb_http.py" "$directory" "$CUR_DIR/../../config/server-cert.pem" "$CUR_DIR/../../config/server-key.pem" > "$directory/ports" 2> "$directory/origin.log" &
origin_pid=$!
table="maxminddb_$CLICKHOUSE_TEST_UNIQUE_NAME"
collection="${table}_source"
query()
{
    $CLICKHOUSE_CLIENT --allow_experimental_maxminddb_table_engine 1 --max_http_get_redirects 5 --send_logs_level fatal -q "$1"
}
cleanup()
{
    query "DROP TABLE IF EXISTS $table SYNC; DROP TABLE IF EXISTS ${table}_redirect SYNC; DROP TABLE IF EXISTS ${table}_local SYNC; DROP TABLE IF EXISTS ${table}_s3 SYNC; DROP TABLE IF EXISTS ${table}_bad SYNC; DROP NAMED COLLECTION IF EXISTS $collection; DROP NAMED COLLECTION IF EXISTS ${collection}_errors;" >/dev/null
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
port=$(python3 -c "import json; print(json.load(open('$directory/ports'))['http'])")
http="http://127.0.0.1:$port"
url="$http/archive?license_key=fixture-api-secret&suffix=tar.gz&opaque={a,b}|c"
query "CREATE TABLE $table ENGINE=MaxMindDB('$url') SETTINGS refresh_interval='100ms'"
query "SELECT version, country.iso_code FROM $table WHERE ip=toIPv4('8.8.8.8')"
query "SELECT position(create_table_query, unhex('666978747572652d6170692d736563726574'))=0 FROM system.tables WHERE database=currentDatabase() AND name='$table' SETTINGS format_display_secrets_in_show_and_select=0"
query "CREATE NAMED COLLECTION $collection AS url='$http/permalink?suffix=tar.gz', headers.header1.name='Authorization', headers.header1.value='Basic dGVzdDp0ZXN0'"
query "CREATE TABLE ${table}_redirect (ip IPv4, version String) ENGINE=MaxMindDB($collection) SETTINGS refresh_interval='100ms'"
query "SELECT version FROM ${table}_redirect WHERE ip=toIPv4('8.8.8.8') SETTINGS max_http_get_redirects=5"

wait_refresh()
{
    python3 - "$directory/stats" <<'PY'
import json
import pathlib
import sys
import time
path = pathlib.Path(sys.argv[1])
before = json.loads(path.read_text())
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    after = json.loads(path.read_text())
    if after['HEAD'] >= before['HEAD'] + 6:
        if after['GET'] != before['GET']:
            raise RuntimeError('Unchanged archive was downloaded during refresh')
        break
else:
    raise RuntimeError('Archive metadata refresh timed out')
PY
}
wait_refresh
echo 'Unchanged archives use metadata only'
set_state()
{
    echo "$1" > "$directory/state.next"
    mv "$directory/state.next" "$directory/state"
}
set_state B
python3 - "$CLICKHOUSE_CLIENT --allow_experimental_maxminddb_table_engine 1" "$table" "${table}_redirect" <<'PY'
import shlex
import subprocess
import sys
import time
command = shlex.split(sys.argv[1])
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    versions = [subprocess.check_output(command + ['-q', f"SELECT version FROM {table} WHERE ip=toIPv4('8.8.8.8')"]).decode().strip() for table in sys.argv[2:]]
    if versions == ['B', 'B']:
        print('Both archives refreshed to B')
        break
else:
    raise RuntimeError('Archive hot reload timed out')
PY
set_state ambiguous
python3 - "$directory/stats" <<'PY'
import json
import pathlib
import sys
import time
path = pathlib.Path(sys.argv[1])
before = json.loads(path.read_text())['GET']
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    if json.loads(path.read_text())['GET'] >= before + 3:
        break
else:
    raise RuntimeError('Invalid archive refresh was not attempted')
PY
query "SELECT version FROM $table WHERE ip=toIPv4('8.8.8.8')"
query "DROP TABLE ${table}_redirect SYNC"
query "CREATE NAMED COLLECTION ${collection}_errors AS url='$url'"
for state in empty ambiguous unsafe invalid oversized corrupt; do
    set_state "$state"
    if query "CREATE TABLE ${table}_bad (ip IPv4) ENGINE=MaxMindDB(${collection}_errors) SETTINGS max_download_size=1024" > "$directory/error.out" 2> "$directory/error.err"; then
        echo "Archive $state unexpectedly accepted" >&2
        exit 1
    fi
    if grep -q 'fixture-api-secret' "$directory/error.err"; then
        echo 'URL credential leaked in exception' >&2
        exit 1
    fi
    if [[ "$state" == oversized ]] && ! grep -q 'after decompression' "$directory/error.err"; then
        cat "$directory/error.err" >&2
        exit 1
    fi
    echo "Archive $state rejected"
done
set_state A
query "ALTER NAMED COLLECTION ${collection}_errors SET url='$http/error?license_key=fixture-api-secret'"
if query "CREATE TABLE ${table}_bad (ip IPv4) ENGINE=MaxMindDB(${collection}_errors)" > "$directory/error.out" 2> "$directory/error.err"; then
    echo 'HTTP failure unexpectedly succeeded' >&2
    exit 1
fi
if grep -q 'fixture-api-secret' "$directory/error.err"; then
    echo 'URL credential leaked in HTTP exception' >&2
    exit 1
fi
echo 'HTTP exception hides URL credential'
if query "SELECT * FROM url(${collection}_errors, format='RawBLOB', structure='data String')" > "$directory/error.out" 2> "$directory/error.err"; then
    echo 'HTTP transport failure unexpectedly succeeded' >&2
    exit 1
fi
if grep -q 'fixture-api-secret' "$directory/error.err"; then
    echo 'URL credential leaked in HTTP transport diagnostics' >&2
    exit 1
fi
echo 'HTTP transport hides reflected URL credential'

relative=$(realpath --relative-to="$CLICKHOUSE_USER_FILES" "$CLICKHOUSE_USER_FILES_UNIQUE")
cp "$directory/A.tar.gz" "$CLICKHOUSE_USER_FILES_UNIQUE/current.tar.gz"
query "CREATE TABLE ${table}_local ENGINE=MaxMindDB('$relative/current.tar.gz') SETTINGS refresh_interval='0'"
query "SELECT version FROM ${table}_local WHERE ip=toIPv4('8.8.8.8')"
s3="http://localhost:11111/test/$CLICKHOUSE_TEST_UNIQUE_NAME.tar.gz"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('$s3', 'test', 'testtest', 'RawBLOB', 'data String') FORMAT RawBLOB" --s3_truncate_on_insert 1 < "$directory/A.tar.gz"
query "CREATE TABLE ${table}_s3 ENGINE=MaxMindDB('$s3', NOSIGN) SETTINGS refresh_interval='0'"
query "SELECT version FROM ${table}_s3 WHERE ip=toIPv4('8.8.8.8')"
