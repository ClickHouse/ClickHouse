#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/pull/110626
# `SQLite` selects the expected columns by name and reads every value as text, so the declared
# column order and types of the `SQLite` table are not a structure mismatch. A genuine value-level
# parse error must not get a misleading structure-mismatch explanation.

phrase='does not match the structure expected by the query'

DB=$(mktemp "$CLICKHOUSE_TMP/insert_schema_mismatch_sqlite_XXXXXX.sqlite")
trap 'rm -f "$DB"' EXIT

$CLICKHOUSE_CLIENT -q 'CREATE TABLE test_sqlite_mismatch (a UInt8, b UInt8) ENGINE = Memory'

echo '-- reordered TEXT columns are read fine'
${CLICKHOUSE_LOCAL} -q "SELECT '2' AS b, '7' AS a FORMAT SQLite" > "$DB"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+test_sqlite_mismatch+FORMAT+SQLite" --data-binary @"$DB"
$CLICKHOUSE_CLIENT -q 'SELECT * FROM test_sqlite_mismatch'

echo '-- value parse error'
${CLICKHOUSE_LOCAL} -q "SELECT '2' AS b, 'x' AS a FORMAT SQLite" > "$DB"
output=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+test_sqlite_mismatch+FORMAT+SQLite" --data-binary @"$DB" 2>&1)
echo "$output" | grep -F -q 'UNEXPECTED_DATA_AFTER_PARSED_VALUE' && echo 'parse error' || echo "unexpected: $output"
echo "$output" | grep -F -q "$phrase" && echo 'explanation present' || echo 'explanation missing'

$CLICKHOUSE_CLIENT -q 'DROP TABLE test_sqlite_mismatch'
