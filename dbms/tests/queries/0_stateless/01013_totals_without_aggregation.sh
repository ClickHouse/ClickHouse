#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

EXCEPTION_SUCCESS_TEXT=ok

# Must throw an exception
EXCEPTION_TEXT="WITH TOTALS, ROLLUP or CUBE are not supported without aggregation"
$CLICKHOUSE_CLIENT --query="SELECT 1 AS id, 'hello' AS s WITH TOTALS" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not throw an exception"
$CLICKHOUSE_CLIENT --query="SELECT 1 AS id, 'hello' AS s WITH ROLLUP" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not throw an exception"
$CLICKHOUSE_CLIENT --query="SELECT 1 AS id, 'hello' AS s WITH CUBE" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not throw an exception"
