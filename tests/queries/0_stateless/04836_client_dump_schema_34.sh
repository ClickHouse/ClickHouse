#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema does not claim that a table's storage expressions fail when its probes could not be sent at all.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT
cd "$work" || exit 1

# An ALIAS that calls `now()` keeps every probe of the table from being sent.
$CLICKHOUSE_LOCAL --path "$work/src" -n -q "CREATE DATABASE d;
    CREATE TABLE d.t (id UInt64, created DateTime, age Int64 ALIAS toInt64(now()) - toInt64(created))
    ENGINE = MergeTree ORDER BY (id, created);"
$CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > "$work/dump.sql" 2> "$work/err.txt" || echo "dump failed: $(cat "$work/err.txt")"
# The typing settings stay unproven, but nothing says the replaying server's profile must change.
grep -h 'typing\|storage expressions' "$work/err.txt"
$CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/dump.sql" > "$work/out.txt" 2>&1
echo "replay rc=$? $(head -c 200 "$work/out.txt")"
