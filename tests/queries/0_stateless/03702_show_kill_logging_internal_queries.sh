#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that internal queries are logged correctly

$CLICKHOUSE_CLIENT --query "SHOW TABLES FORMAT Null" --trace_profile_events 1 --trace_profile_events_list 'Query'
$CLICKHOUSE_CLIENT --query "SHOW ENGINES FORMAT Null" --trace_profile_events 1 --trace_profile_events_list 'Query'
$CLICKHOUSE_CLIENT --query "SHOW FUNCTIONS LIKE 'plus' FORMAT Null" --trace_profile_events 1 --trace_profile_events_list 'Query'
$CLICKHOUSE_CLIENT --query "SHOW SETTING max_threads FORMAT Null" --trace_profile_events 1 --trace_profile_events_list 'Query'
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = 'nonexistent' SYNC" --trace_profile_events 1 --trace_profile_events_list 'Query' &>/dev/null

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log, trace_log"

# Verify that the internal queries have been logged to system.query_log
# and that trace_log entries carry the internal query's query_id (not the outer query's).
$CLICKHOUSE_CLIENT --query "
SELECT
    countIf(query LIKE '%system.tables%' AND type = 'QueryStart'),
    countIf(query LIKE '%system.tables%' AND type = 'QueryFinish'),
    countIf(query LIKE '%system.table_engines%' AND type = 'QueryStart'),
    countIf(query LIKE '%system.table_engines%' AND type = 'QueryFinish'),
    countIf(query LIKE '%system.functions%' AND type = 'QueryStart'),
    countIf(query LIKE '%system.functions%' AND type = 'QueryFinish'),
    countIf(query LIKE '%system.settings%' AND type = 'QueryStart'),
    countIf(query LIKE '%system.settings%' AND type = 'QueryFinish'),
    countIf(query LIKE '%system.processes%' AND type = 'QueryStart'),
    countIf(query LIKE '%system.processes%' AND type = 'QueryFinish')
FROM system.query_log
WHERE is_internal = 1 AND current_database = currentDatabase()
  AND query_id IN (
    SELECT query_id
    FROM system.trace_log
    WHERE trace_type = 'ProfileEvent' AND event = 'Query'
  )"
