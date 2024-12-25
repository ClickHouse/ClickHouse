#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TIMEOUT=5
IS_SANITIZER_OR_DEBUG=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.warnings WHERE message like '%built with sanitizer%' or message like '%built in debug mode%'")
if [ "$IS_SANITIZER_OR_DEBUG" -gt 0 ]; then
    # Increase the timeout due to in debug/sanitizers build:
    # - client is slow
    # - stacktrace resolving is slow
    TIMEOUT=15
fi

# TCP CLIENT: As of today (02/12/21) uses PullingAsyncPipelineExecutor
### Should be cancelled after 1 second and return a 159 exception (timeout)
### However, in the test, the server can be overloaded, so we assert query duration in the interval of 1 to 60 seconds.
query_id=$(random_str 12)
$CLICKHOUSE_CLIENT --max_result_rows 0 --max_result_bytes 0 --query_id "$query_id" --max_execution_time 1 -q "
    SELECT * FROM
    (
        SELECT a.name as n
        FROM
        (
            SELECT 'Name' as name, number FROM system.numbers LIMIT 2000000
        ) AS a,
        (
            SELECT 'Name' as name2, number FROM system.numbers LIMIT 2000000
        ) as b
        GROUP BY n
    )
    LIMIT 20
    FORMAT Null
" 2>&1 | grep -m1 -o "Code: 159"
$CLICKHOUSE_CLIENT -q "system flush logs"
${CLICKHOUSE_CURL} -q -sS "$CLICKHOUSE_URL" -d "select 'query_duration', round(query_duration_ms/1000) BETWEEN 1 AND 60 from system.query_log where current_database = '$CLICKHOUSE_DATABASE' and query_id = '$query_id' and type != 'QueryStart'"


### Should stop pulling data and return what has been generated already (return code 0)
query_id=$(random_str 12)
$CLICKHOUSE_CLIENT --max_result_rows 0 --max_result_bytes 0 --query_id "$query_id" -q "
    SELECT a.name as n
    FROM
    (
        SELECT 'Name' as name, number FROM system.numbers LIMIT 2000000
    ) AS a,
    (
        SELECT 'Name' as name2, number FROM system.numbers LIMIT 2000000
    ) as b
    FORMAT Null
    SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break'
"
echo $?
$CLICKHOUSE_CLIENT -q "system flush logs"
${CLICKHOUSE_CURL} -q -sS "$CLICKHOUSE_URL" -d "select 'query_duration', round(query_duration_ms/1000) BETWEEN 1 AND 60 from system.query_log where current_database = '$CLICKHOUSE_DATABASE' and query_id = '$query_id' and type != 'QueryStart'"


# HTTP CLIENT: As of today (02/12/21) uses PullingPipelineExecutor
### Should be cancelled after 1 second and return a 159 exception (timeout)
${CLICKHOUSE_CURL} -q --max-time $TIMEOUT -sS "$CLICKHOUSE_URL&max_execution_time=1&max_result_rows=0&max_result_bytes=0" -d "
    SELECT * FROM
    (
        SELECT a.name as n
        FROM
        (
            SELECT 'Name' as name, number FROM system.numbers LIMIT 2000000
        ) AS a,
        (
            SELECT 'Name' as name2, number FROM system.numbers LIMIT 2000000
        ) as b
        GROUP BY n
    )
    LIMIT 20
    FORMAT Null
" 2>&1 | grep -o "Code: 159" | sort | uniq


### Should stop pulling data and return what has been generated already (return code 0)
${CLICKHOUSE_CURL} -q --max-time $TIMEOUT -sS "$CLICKHOUSE_URL&max_result_rows=0&max_result_bytes=0" -d "
    SELECT a.name as n
          FROM
          (
              SELECT 'Name' as name, number FROM system.numbers LIMIT 2000000
          ) AS a,
          (
              SELECT 'Name' as name2, number FROM system.numbers LIMIT 2000000
          ) as b
          FORMAT Null
          SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break'
"
echo $?
