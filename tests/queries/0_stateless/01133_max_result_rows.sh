#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function check_rows
{
    output=$1
    max=$2

    if [[ $(echo "$output" | wc -l) -gt $max ]]; then
        echo "error"
    else
        echo "ok"
    fi
}

SETTINGS="SETTINGS max_block_size = 10, max_result_rows = 20, result_overflow_mode = 'throw'"

# When using result_overflow_mode=throw ClickHouse does not guarantee the number of rows returned
# is exactly the maximum. For that behavior, result_overflow_mode=break should be used.
echo "-- Check that throw works and that the rows are at most the limit set"
output=$($CLICKHOUSE_CLIENT --query "SELECT DISTINCT intDiv(number, 10) FROM numbers(300) $SETTINGS; -- { serverError 396 }")
check_rows "$output" 20
output=$($CLICKHOUSE_CLIENT --query "SELECT DISTINCT intDiv(number, 10) FROM numbers(210) $SETTINGS; -- { serverError 396 }")
check_rows "$output" 20

echo "-- Check that nothing is thrown if the limit is not reached"
$CLICKHOUSE_CLIENT --query "SELECT DISTINCT intDiv(number, 10) FROM numbers(190) $SETTINGS;"
$CLICKHOUSE_CLIENT --query "SELECT DISTINCT intDiv(number, 10) FROM numbers(200) $SETTINGS;"

SETTINGS="SETTINGS max_block_size = 10, max_result_rows = 20, result_overflow_mode = 'break'"

echo "-- Check that result_overflow_mode = 'break' works"
$CLICKHOUSE_CLIENT --query "SELECT DISTINCT intDiv(number, 10) FROM numbers(300) $SETTINGS;"
$CLICKHOUSE_CLIENT --query "SELECT DISTINCT intDiv(number, 10) FROM numbers(190) $SETTINGS;"
$CLICKHOUSE_CLIENT --query "SELECT DISTINCT intDiv(number, 10) FROM numbers(200) $SETTINGS;"
$CLICKHOUSE_CLIENT --query "SELECT DISTINCT intDiv(number, 10) FROM numbers(210) $SETTINGS;"

SETTINGS="SETTINGS max_block_size = 10, max_result_rows = 1, result_overflow_mode = 'break'"
$CLICKHOUSE_CLIENT --query "SELECT number FROM system.numbers $SETTINGS;"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM numbers(100) $SETTINGS;"

echo "-- Check that subquery result is not the total result"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM (SELECT * FROM numbers(100)) $SETTINGS;"
echo "-- Check that remote query result is not the total result"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM remote('127.0.0.{1,2}', numbers(100)) $SETTINGS;"
