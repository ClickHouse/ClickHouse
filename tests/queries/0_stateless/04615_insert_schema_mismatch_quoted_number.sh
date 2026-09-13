#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# The schema-mismatch diagnostic must not treat a quoted numeric string as a structure mismatch for a
# numeric column: formats that read values from text (`JSONEachRow`, `CSV`, ...) accept a quoted number
# such as "1" into a numeric column even though schema inference keeps it as `String`. It must, however,
# still explain a genuinely non-numeric string (e.g. "hello") for a numeric column. Both are told apart
# by inferring the schema a second time with number-from-string inference enabled.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_quoted_number (ok UInt8, bad UInt8) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_quoted_number_text (a Int64) ENGINE = Memory"

echo "-- JSONEachRow, quoted numeric string is valid for a numeric column; only the second column fails (no false positive)"
printf 'INSERT INTO test_quoted_number FORMAT JSONEachRow\n{"ok": "1", "bad": 1.5}\n' \
    | $CLICKHOUSE_CLIENT --async_insert 0 2>&1 | check

echo "-- JSONEachRow, genuinely non-numeric string for a numeric column is still explained"
printf 'INSERT INTO test_quoted_number_text FORMAT JSONEachRow\n{"a": "hello"}\n' \
    | $CLICKHOUSE_CLIENT --async_insert 0 2>&1 | check

echo "-- clickhouse-local, CSV, quoted numeric string is valid for a numeric column; only the second column fails (no false positive)"
printf 'CREATE TABLE t (ok UInt8, bad UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSV\n"1",1.5\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

$CLICKHOUSE_CLIENT -q "DROP TABLE test_quoted_number"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_quoted_number_text"
