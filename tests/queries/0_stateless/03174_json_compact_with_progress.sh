#!/usr/bin/env bash
# Tags: no-parallel-replicas
# ^ there is a bug: when using parallel replicas, rows_before_limit_at_least is always output, while it shouldn't.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_table;"

$CLICKHOUSE_CLIENT -q "SELECT 'Check JSONCompactEachRowWithProgress';"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_table (value UInt8, name String, arr Array(UInt64)) ENGINE = MergeTree() ORDER BY value;"
$CLICKHOUSE_CLIENT -q "INSERT INTO test_table VALUES (1, 'a', []), (2, 'b', [123]), (3, 'c', [456, 789]);"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_table FORMAT JSONCompactEachRowWithProgress settings max_block_size=2;" |  grep -v --text "progress"

$CLICKHOUSE_CLIENT -q "SELECT 'Check JSONCompactStringsEachRowWithProgress';"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_table FORMAT JSONCompactStringsEachRowWithProgress settings max_block_size=2;" |  grep -v --text "progress"

$CLICKHOUSE_CLIENT -q "SELECT 'Check totals';"
$CLICKHOUSE_CLIENT -q "SELECT name, count() AS c FROM test_table GROUP BY name WITH TOTALS ORDER BY name FORMAT JSONCompactEachRowWithProgress settings max_block_size=2;" | grep -v --text "progress"

$CLICKHOUSE_CLIENT -q "SELECT 'Check exceptions';"
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "SELECT throwIf(number = 15), 1::Int64 as a, '\"' from numbers(100) format JSONCompactEachRowWithProgress settings output_format_json_quote_64bit_integers=1, max_block_size=10, http_write_exception_in_output_format=1" | grep "exception" | sed -r -e 's/("exception":"Code: [0-9]+)[^"]+(")/\1\2/'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_table;"
