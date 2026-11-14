#!/usr/bin/env bash
# Tags: use_maxminddb

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail
mkdir -p "$CLICKHOUSE_USER_FILES_UNIQUE"
python3 "$CUR_DIR/helpers/maxminddb.py" "$CLICKHOUSE_USER_FILES_UNIQUE"
python3 - "$CUR_DIR/helpers" "$CLICKHOUSE_USER_FILES_UNIQUE" <<'PY'
import pathlib
import sys

sys.path.insert(0, sys.argv[1])
from maxminddb import Value, encode, write_database

directory = pathlib.Path(sys.argv[2])
path = directory / 'v4_B.mmdb'
prefix, marker, metadata = path.read_bytes().rpartition(b'\xab\xcd\xefMaxMind.com')
assert marker
metadata = metadata.replace(encode(Value(9, 1790899200)), encode(Value(9, 1790985600)))
metadata = metadata.replace(encode('ClickHouse-MaxMindDB-Test'), encode('ClickHouse-MaxMindDB-Test-B'))
path.write_bytes(prefix + marker + metadata)
write_database(directory / 'reserved_metadata.mmdb', 4, [('8.8.8.0/24', {'_mmdb_metadata': 'reserved'})])
PY

table="maxminddb_metadata_${CLICKHOUSE_TEST_UNIQUE_NAME}"
relative="$CLICKHOUSE_TEST_UNIQUE_NAME"
query()
{
    $CLICKHOUSE_CLIENT --allow_experimental_maxminddb_table_engine 1 --print_pretty_type_names 0 -q "$1"
}
cleanup()
{
    query "DROP TABLE IF EXISTS $table SYNC; DROP TABLE IF EXISTS ${table}_manual SYNC; DROP TABLE IF EXISTS ${table}_v6 SYNC" >/dev/null
    rm -rf "$CLICKHOUSE_USER_FILES_UNIQUE"
}
trap cleanup EXIT

cp "$CLICKHOUSE_USER_FILES_UNIQUE/v4_A.mmdb" "$CLICKHOUSE_USER_FILES_UNIQUE/current.mmdb"
query "CREATE TABLE $table ENGINE=MaxMindDB('$relative/current.mmdb') SETTINGS refresh_interval='100ms'"
query "SELECT _mmdb_metadata FROM $table WHERE ip=toIPv4('8.8.8.8') FORMAT JSONEachRow"
query "SELECT toTypeName(_mmdb_metadata) FROM $table WHERE ip=toIPv4('8.8.8.8')"
query "SELECT _mmdb_metadata.languages.size0, _mmdb_metadata.description.keys, _mmdb_metadata.description.values FROM $table WHERE ip=toIPv4('8.8.8.8')"
query "SELECT ip, _mmdb_metadata.database_type, _mmdb_metadata.ip_version, _mmdb_metadata.build_epoch, _mmdb_metadata.languages, _mmdb_metadata.description['en'] FROM $table WHERE ip IN (toIPv4('8.8.8.8'), toIPv4('1.1.1.1')) ORDER BY ip SETTINGS max_block_size=1"
query "SELECT count(), uniqExact(_mmdb_metadata.build_epoch) FROM $table WHERE ip IN (toIPv4('8.8.8.8'), toIPv4('1.1.1.1')) AND _mmdb_metadata.build_epoch=1790899200"
query "SELECT _mmdb_metadata FROM $table WHERE ip=toIPv4('9.9.9.9')"
[[ $(query "SELECT * FROM $table WHERE ip=toIPv4('8.8.8.8') FORMAT JSONEachRow") != *'"_mmdb_metadata"'* ]]
echo 'Virtual metadata excluded from SELECT *'
query "CREATE TABLE ${table}_manual (ip IPv4) ENGINE=MaxMindDB('$relative/v4_A.mmdb') SETTINGS refresh_interval='0'"
query "SELECT _mmdb_metadata.ip_version FROM ${table}_manual WHERE ip=toIPv4('8.8.8.8')"
query "CREATE TABLE ${table}_v6 ENGINE=MaxMindDB('$relative/v6_A.mmdb') SETTINGS refresh_interval='0'"
query "SELECT _mmdb_metadata.ip_version FROM ${table}_v6 WHERE ip=toIPv6('2001:db8::1')"
query "SELECT l.ip, r._mmdb_metadata.database_type, r._mmdb_metadata.build_epoch FROM (SELECT arrayJoin([toIPv4('8.8.8.8'), toIPv4('9.9.9.9')]) AS ip) AS l LEFT ANY JOIN $table AS r USING ip ORDER BY l.ip SETTINGS join_algorithm='direct', enable_parallel_replicas=0"

expect_error()
{
    local sql="$1"
    local actual=0
    query "$sql" >"$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}.out" 2>"$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}.err" || actual=$?
    [[ "$actual" == 36 ]] && grep -q 'Code: 36\.' "$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}.err" || { cat "$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}.err" >&2; exit 1; }
    echo 'Error 36'
}
expect_error "SELECT _mmdb_metadata FROM $table"
expect_error "CREATE TABLE ${table}_bad (ip IPv4, _mmdb_metadata String) ENGINE=MaxMindDB('$relative/v4_A.mmdb')"
expect_error "CREATE TABLE ${table}_bad (ip IPv4, \`_mmdb_metadata.build_epoch\` UInt64) ENGINE=MaxMindDB('$relative/v4_A.mmdb')"
expect_error "CREATE TABLE ${table}_bad ENGINE=MaxMindDB('$relative/reserved_metadata.mmdb')"
expect_error "CREATE TABLE ${table}_bad (ip IPv4) ENGINE=MaxMindDB('$relative/reserved_metadata.mmdb')"

cp "$CLICKHOUSE_USER_FILES_UNIQUE/v4_B.mmdb" "$CLICKHOUSE_USER_FILES_UNIQUE/next.mmdb"
mv "$CLICKHOUSE_USER_FILES_UNIQUE/next.mmdb" "$CLICKHOUSE_USER_FILES_UNIQUE/current.mmdb"
python3 - "$CLICKHOUSE_CLIENT --allow_experimental_maxminddb_table_engine 1" "$table" <<'PY'
import shlex
import subprocess
import sys
import time

command, table = sys.argv[1:]
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    result = subprocess.check_output(shlex.split(command) + ['-q', f"SELECT version, _mmdb_metadata.build_epoch, _mmdb_metadata.database_type FROM {table} WHERE ip=toIPv4('8.8.8.8')"]).decode().strip()
    if result.startswith('B\t'):
        assert result == 'B\t1790985600\tClickHouse-MaxMindDB-Test-B', result
        print('Reloaded payload and metadata from generation B')
        break
else:
    raise RuntimeError('Metadata refresh timed out')
PY
query "SELECT _mmdb_metadata.build_epoch FROM ${table}_manual WHERE ip=toIPv4('8.8.8.8')"
rm -f "$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}.out" "$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"
