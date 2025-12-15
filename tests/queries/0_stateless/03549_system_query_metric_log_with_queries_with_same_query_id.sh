#!/bin/bash
# Tags: no-msan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

readonly query_prefix=$CLICKHOUSE_DATABASE
readonly same_query_not_finish="${query_prefix}_not_two_finish"
readonly same_query_finish="${query_prefix}_two_finish"

# We launch two queries so that the second is not run because the first one is still running.
$CLICKHOUSE_CLIENT --query-id="$same_query_not_finish" -q "SELECT sleep(120) SETTINGS function_sleep_max_microseconds_per_block=1000000000, query_metric_log_interval=100, replace_running_query=0 FORMAT Null; -- { serverError QUERY_WAS_CANCELLED}" &

# In this case, we let the first one running a little bit and run the second one so that it cancels the first one.
$CLICKHOUSE_CLIENT --query-id="$same_query_finish" -q "SELECT sleep(120) SETTINGS function_sleep_max_microseconds_per_block=1000000000, query_metric_log_interval=0, replace_running_query=0 FORMAT Null; -- { serverError QUERY_WAS_CANCELLED}" &

# Ensure both queries have started
while true; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS system.query_log;"
    count=$($CLICKHOUSE_CLIENT -q """
        SELECT
            count()
        FROM
            system.query_log
        WHERE
            event_date >= yesterday()
            AND current_database = currentDatabase()
            AND (query_id = '$same_query_not_finish' OR query_id = '$same_query_finish')
            AND query ILIKE 'SELECT %'
            AND type = 'QueryStart';
    """)

    if [[ $count -ge 2 ]]; then
        break
    fi
    sleep 0.5
done

$CLICKHOUSE_CLIENT --query-id="$same_query_finish" -q "SELECT sleep(2) SETTINGS query_metric_log_interval=100, replace_running_query=1 FORMAT Null;" &
$CLICKHOUSE_CLIENT --query-id="$same_query_not_finish" -q "SELECT 'a' SETTINGS query_metric_log_interval=0, replace_running_query=0 FORMAT Null;" 2> /dev/null

# Kill the initial query because the second one didn't replace it
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$same_query_not_finish' SYNC FORMAT Null" &

wait

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log, query_metric_log"

# We check that the first query collected enough metrics
$CLICKHOUSE_CLIENT -q """
    SELECT '-- Ensure the second query with same query_id did not finish the first one';
    SELECT
        countIf(type = 'QueryStart'), countIf(type = 'QueryFinish'), countIf(type = 'ExceptionBeforeStart'), countIf(type = 'ExceptionWhileProcessing')
    FROM
        system.query_log
    WHERE
        event_date >= yesterday()
        AND query_id = '$same_query_not_finish'
        AND query LIKE 'SELECT%'
        AND current_database = currentDatabase();
    SELECT if(count() > 1, 'ok', 'error') FROM system.query_metric_log WHERE event_date >= yesterday() AND query_id = '$same_query_not_finish';
"""

# We check that there's enough metrics collected by the second query. Note we cannot really
# distinguish from which of the two queries it comes from, though. We rely on the
# query_metric_log_interval=0 to ensure that one doesn't collect anything.
$CLICKHOUSE_CLIENT -q """
    SELECT '-- Ensure the second query cancels the previous one and runs to completion';
    SELECT
        countIf(type = 'QueryStart'), countIf(type = 'QueryFinish'), countIf(type = 'ExceptionBeforeStart'), countIf(type = 'ExceptionWhileProcessing')
    FROM
        system.query_log
    WHERE
        event_date >= yesterday()
        AND query_id = '$same_query_finish'
        AND query LIKE 'SELECT%'
        AND current_database = currentDatabase();
    SELECT if(count() >= 1, 'ok', 'error') FROM system.query_metric_log WHERE event_date >= yesterday() AND query_id = '$same_query_finish';
"""
