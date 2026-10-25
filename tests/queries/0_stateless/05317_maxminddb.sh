#!/usr/bin/env bash
# Tags: use_maxminddb

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

mkdir -p "$CLICKHOUSE_USER_FILES_UNIQUE"
python3 "$CUR_DIR/helpers/maxminddb.py" "$CLICKHOUSE_USER_FILES_UNIQUE"
relative="${CLICKHOUSE_TEST_UNIQUE_NAME}"
table="maxminddb_${CLICKHOUSE_TEST_UNIQUE_NAME}"

query()
{
    $CLICKHOUSE_CLIENT --allow_experimental_maxminddb_table_engine 1 --print_pretty_type_names 0 -q "$1"
}

cleanup()
{
    [[ -z "${reader_pid:-}" ]] || kill "$reader_pid" 2>/dev/null || true
    [[ -z "${lookup_pid:-}" ]] || kill "$lookup_pid" 2>/dev/null || true
    query "DROP TABLE IF EXISTS $table SYNC; DROP TABLE IF EXISTS ${table}_manual SYNC; DROP TABLE IF EXISTS ${table}_v6 SYNC; DROP TABLE IF EXISTS ${table}_v4_in_v6 SYNC; DROP TABLE IF EXISTS ${table}_disabled SYNC; DROP TABLE IF EXISTS ${table}_duration SYNC; DROP TABLE IF EXISTS ${table}_probe SYNC;" >/dev/null
    rm -rf "$CLICKHOUSE_USER_FILES_UNIQUE"
    [[ -z "${pipe_dir:-}" ]] || rm -rf "$pipe_dir"
}
trap cleanup EXIT

cp "$CLICKHOUSE_USER_FILES_UNIQUE/v4_A.mmdb" "$CLICKHOUSE_USER_FILES_UNIQUE/current.mmdb"
query "CREATE TABLE $table ENGINE = MaxMindDB('$relative/current.mmdb') SETTINGS refresh_interval='100ms'"
query "DESCRIBE TABLE $table FORMAT TSV"
query "SELECT ip, version, country.iso_code, country.names.fr, location.longitude, array, nested.value, bool, hex(bytes), float, uint16, uint32, uint64, uint128, int32, promoted, empty FROM $table WHERE ip = toIPv4('8.8.8.8') FORMAT TSV"
query "SELECT ip, country.iso_code, country.names.fr, location.longitude, array, bool, promoted FROM $table WHERE ip IN (toIPv4('8.8.8.8'), toIPv4('8.8.8.9'), toIPv4('8.8.9.9'), toIPv4('1.1.1.1'), toIPv4('8.8.8.8'), toIPv4('9.9.9.9')) ORDER BY ip"
query "SELECT ip FROM $table WHERE ip IN (SELECT toIPv4(number) FROM numbers(0))"
query "SELECT country.iso_code FROM $table WHERE ip = '8.8.8.8' AND version = 'missing'"
query "SELECT toTypeName(country.iso_code), toTypeName(array), toTypeName(bool), toTypeName(promoted) FROM $table WHERE ip = toIPv4('8.8.8.8')"
query "SELECT ip, optional.label, widened, wide_mixed, toTypeName(optional), toTypeName(widened), toTypeName(wide_mixed) FROM $table WHERE ip IN (toIPv4('1.1.1.1'), toIPv4('8.8.8.8')) ORDER BY ip"
query "CREATE TABLE ${table}_manual (ip IPv4, country Tuple(iso_code Nullable(String), names Map(String, String)), location Tuple(latitude Float64, longitude Nullable(Float64))) ENGINE = MaxMindDB('$relative/v4_A.mmdb') PRIMARY KEY ip SETTINGS refresh_interval='0'"
query "SELECT country.iso_code, country.names['fr'], location.longitude FROM ${table}_manual WHERE ip = toIPv4('8.8.8.8')"
query "CREATE TABLE ${table}_v6 ENGINE = MaxMindDB('$relative/v6_A.mmdb') SETTINGS refresh_interval='0'"
query "SELECT ip, country.iso_code, bool FROM ${table}_v6 WHERE ip IN (toIPv6('2001:db8::1'), toIPv6('2001:db8:1::1'), toIPv6('::8.8.8.8'), toIPv6('::ffff:8.8.8.8')) ORDER BY ip"
query "CREATE TABLE ${table}_v4_in_v6 (ip IPv4, version String) ENGINE = MaxMindDB('$relative/v6_A.mmdb') SETTINGS refresh_interval='0'"
query "SELECT ip, version FROM ${table}_v4_in_v6 WHERE ip=toIPv4('8.8.8.8')"
expect_error()
{
    local code="$1"
    local sql="$2"
    local actual=0
    query "$sql" >"$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}.out" 2>"$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}.err" || actual=$?
    [[ "$actual" == "$((code % 256))" ]] && grep -q "Code: $code\\." "$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}.err" || { cat "$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}.err" >&2; exit 1; }
    echo "Error $code"
}
expect_error 36 "SELECT * FROM $table"
expect_error 36 "SELECT * FROM $table WHERE ip > toIPv4('8.8.8.8')"
expect_error 36 "CREATE TABLE ${table}_bad (ip String) ENGINE=MaxMindDB('$relative/v4_A.mmdb')"
expect_error 36 "CREATE TABLE ${table}_bad (key IPv4) ENGINE=MaxMindDB('$relative/v4_A.mmdb')"
expect_error 36 "CREATE TABLE ${table}_bad (ip IPv6) ENGINE=MaxMindDB('$relative/v4_A.mmdb')"
expect_error 36 "CREATE TABLE ${table}_bad (ip IPv4, version String) ENGINE=MaxMindDB('$relative/v4_A.mmdb') PRIMARY KEY version"
expect_error 400 "CREATE TABLE ${table}_bad ENGINE=MaxMindDB('$relative/missing.mmdb')"
expect_error 117 "CREATE TABLE ${table}_bad ENGINE=MaxMindDB('$relative/invalid.mmdb')"
expect_error 53 "CREATE TABLE ${table}_bad (ip IPv4, country UInt64) ENGINE=MaxMindDB('$relative/v4_A.mmdb')"
expect_error 53 "CREATE TABLE ${table}_bad (ip IPv4, uint32 UInt16) ENGINE=MaxMindDB('$relative/v4_A.mmdb')"
expect_error 386 "CREATE TABLE ${table}_bad ENGINE=MaxMindDB('$relative/incompatible_inference.mmdb')"
expect_error 36 "CREATE TABLE ${table}_bad ENGINE=MaxMindDB('$relative/reserved.mmdb')"
expect_error 291 "CREATE TABLE ${table}_bad (ip IPv4) ENGINE=MaxMindDB('/etc/passwd')"
expect_error 291 "CREATE TABLE ${table}_bad (ip IPv4) ENGINE=MaxMindDB('$relative/../../../../../etc/passwd')"
ln -s /etc/passwd "$CLICKHOUSE_USER_FILES_UNIQUE/outside.mmdb"
expect_error 291 "CREATE TABLE ${table}_bad (ip IPv4) ENGINE=MaxMindDB('$relative/outside.mmdb')"
expect_error 36 "CREATE TABLE ${table}_bad (ip IPv4) ENGINE=MaxMindDB('$relative/v4_A.mmdb') SETTINGS refresh_interval='-1ms'"
for duration in 5m 10m 20h; do
    query "CREATE TABLE ${table}_duration (ip IPv4) ENGINE=MaxMindDB('$relative/v4_A.mmdb') SETTINGS refresh_interval='$duration'; DROP TABLE ${table}_duration SYNC"
    echo "Duration $duration"
done

wait_version()
{
    python3 - "$CLICKHOUSE_CLIENT --allow_experimental_maxminddb_table_engine 1" "$table" "$1" <<'PY'
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
    raise RuntimeError('MaxMindDB refresh timed out')
PY
}

# Validate local hot reload independently of Direct JOIN.
cp "$CLICKHOUSE_USER_FILES_UNIQUE/v4_B.mmdb" "$CLICKHOUSE_USER_FILES_UNIQUE/next.mmdb"
mv "$CLICKHOUSE_USER_FILES_UNIQUE/next.mmdb" "$CLICKHOUSE_USER_FILES_UNIQUE/current.mmdb"
wait_version B

# A failed generation must not publish a new schema or replace valid data.
started=$(query 'SELECT toUnixTimestamp(now())')
cp "$CLICKHOUSE_USER_FILES_UNIQUE/incompatible.mmdb" "$CLICKHOUSE_USER_FILES_UNIQUE/next.mmdb"
mv "$CLICKHOUSE_USER_FILES_UNIQUE/next.mmdb" "$CLICKHOUSE_USER_FILES_UNIQUE/current.mmdb"
python3 - "$CLICKHOUSE_CLIENT" "$table" "$started" <<'PY'
import shlex
import subprocess
import sys
import time

command, table, started = sys.argv[1:]
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    value = subprocess.check_output(shlex.split(command) + ['--multiquery', '-q',
        "SYSTEM FLUSH LOGS text_log; SELECT count() FROM system.text_log "
        "WHERE event_date >= toDate(toDateTime(" + started + ")) AND event_time >= toDateTime(" + started + ") "
        "AND logger_name='StorageMaxMindDB' AND position(message, '" + table + "') > 0 "
        "AND position(message, 'failed during schema validation') > 0"]).decode().strip()
    if int(value) > 0:
        break
else:
    raise RuntimeError('Incompatible MaxMindDB generation was not validated')
PY
query "SELECT version FROM $table WHERE ip=toIPv4('8.8.8.8')"
cp "$CLICKHOUSE_USER_FILES_UNIQUE/v4_A.mmdb" "$CLICKHOUSE_USER_FILES_UNIQUE/next.mmdb"
mv "$CLICKHOUSE_USER_FILES_UNIQUE/next.mmdb" "$CLICKHOUSE_USER_FILES_UNIQUE/current.mmdb"
wait_version A

query "CREATE TABLE ${table}_disabled (ip IPv4, version String) ENGINE=MaxMindDB('$relative/current.mmdb') SETTINGS refresh_interval='0'"
cp "$CLICKHOUSE_USER_FILES_UNIQUE/v4_B.mmdb" "$CLICKHOUSE_USER_FILES_UNIQUE/next.mmdb"
mv "$CLICKHOUSE_USER_FILES_UNIQUE/next.mmdb" "$CLICKHOUSE_USER_FILES_UNIQUE/current.mmdb"
wait_version B
query "SELECT version FROM ${table}_disabled WHERE ip=toIPv4('8.8.8.8')"
query "DESCRIBE TABLE $table FORMAT TSV"
rm -f "$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}.out" "$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"
