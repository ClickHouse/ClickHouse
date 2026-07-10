#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the test compares global error counters in system.errors before and after
# an INSERT; a concurrent test could increment the same counters and produce false diffs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Values format first parses each value with a fast streaming parser and falls back to the
# SQL expression parser when the value is not a plain literal. The fallback must not construct
# exceptions: every constructed exception is counted in system.errors (and system.error_log)
# even when it is caught and handled, producing confusing error records for successful queries.
#
# The inserts go through HTTP so that the data is parsed on the server side
# (clickhouse-client parses inline Values data on the client side).

PARSE_ERROR_NAMES="'CANNOT_PARSE_INPUT_ASSERTION_FAILED', 'CANNOT_PARSE_QUOTED_STRING', 'CANNOT_PARSE_NUMBER',
    'CANNOT_PARSE_DATE', 'CANNOT_PARSE_DATETIME', 'CANNOT_PARSE_BOOL', 'CANNOT_PARSE_UUID',
    'CANNOT_READ_ARRAY_FROM_TEXT', 'CANNOT_READ_MAP_FROM_TEXT', 'CANNOT_READ_ALL_DATA',
    'ARGUMENT_OUT_OF_BOUND', 'INCORRECT_DATA', 'UNEXPECTED_DATA_AFTER_PARSED_VALUE'"

function get_parse_error_counters()
{
    $CLICKHOUSE_CLIENT -q "SELECT name, value FROM system.errors WHERE name IN (${PARSE_ERROR_NAMES}) ORDER BY name"
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_values_fallback (x UInt64, s String, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY x"

counters_before=$(get_parse_error_counters)

# Expressions, NULL as default, DEFAULT keyword, a non-DEFAULT value starting with 'd',
# and a Map given as an escaped string: all of them miss the streaming fast path
# (or take its special branches) and must not increment any error counter.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary \
    "INSERT INTO t_values_fallback VALUES (1 + 1, 'a', {'k1' : 1}), (3, upper('b'), {}), (NULL, 'c', '{\'k2\' : 2}'), (DEFAULT, 'd', {}), (divide(9, 3), 'div', {})"

counters_after=$(get_parse_error_counters)

echo '--- inserted data ---'
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_values_fallback ORDER BY x, s"

echo '--- parse error counters ---'
if [ "$counters_before" == "$counters_after" ]; then
    echo 'no new parse errors'
else
    echo 'parse error counters changed:'
    diff <(echo "$counters_before") <(echo "$counters_after")
fi

# Decimal columns keep the old behavior: an overflowing decimal literal must fail the query
# instead of being read as a Float64 expression that would silently lose precision.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_values_decimal (d Decimal32(2)) ENGINE = MergeTree ORDER BY d"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "INSERT INTO t_values_decimal VALUES (1.23), (1.20 + 0.03)"

echo '--- decimal overflow still fails ---'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "INSERT INTO t_values_decimal VALUES (12345678.91)" | grep -o "ARGUMENT_OUT_OF_BOUND" | head -1

echo '--- decimal data ---'
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_values_decimal ORDER BY d"
