#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that show_processlist_include_internal setting works correctly

RUN_ID="test_${RANDOM}_${RANDOM}_$$"

$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.slow_dict
(
    key UInt64,
    value String
)
PRIMARY KEY key
SOURCE(
    CLICKHOUSE(
        QUERY 'SELECT number AS key, toString(number) AS value FROM numbers(10) WHERE sleep(3600) = 0 AND ''$RUN_ID'' != '''''
    )
)
LAYOUT(DIRECT())
SETTINGS(function_sleep_max_microseconds_per_block = 3600000000)"

# this starts an internal query
$CLICKHOUSE_CLIENT --query "SELECT * FROM ${CLICKHOUSE_DATABASE}.slow_dict FORMAT Null" &
ASYNC_DICT_QUERY_PID=$!

# wait for the internal query to start
for _ in {1..10}; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
    SELECT
        count()
    FROM system.query_log
    WHERE 1
        AND type = 'QueryStart'
        AND is_internal = 1
        AND query LIKE '%$RUN_ID%'
        AND query_kind = 'Select'
        AND current_database IN ['default', currentDatabase()]
    " | grep -q '^1$' && break
    sleep 0.05
done

$CLICKHOUSE_CLIENT --query "SHOW PROCESSLIST" | grep -c "$RUN_ID"
$CLICKHOUSE_CLIENT --query "SHOW PROCESSLIST SETTINGS show_processlist_include_internal = 1" | grep -c "$RUN_ID"

kill $ASYNC_DICT_QUERY_PID
$CLICKHOUSE_CLIENT --query "DROP DICTIONARY ${CLICKHOUSE_DATABASE}.slow_dict"
