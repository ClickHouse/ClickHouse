#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that internal queries are logged correctly

TEST_USER="test_user_${RANDOM}_${RANDOM}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${TEST_USER}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${TEST_USER}"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON system.* TO ${TEST_USER}"

$CLICKHOUSE_CLIENT --user ${TEST_USER} --query "SHOW TABLES FORMAT Null"
$CLICKHOUSE_CLIENT --user ${TEST_USER} --query "SHOW ENGINES FORMAT Null"
$CLICKHOUSE_CLIENT --user ${TEST_USER} --query "SHOW FUNCTIONS LIKE 'plus' FORMAT Null"
$CLICKHOUSE_CLIENT --user ${TEST_USER} --query "SHOW SETTING max_threads FORMAT Null"
$CLICKHOUSE_CLIENT --user ${TEST_USER} --query "KILL QUERY WHERE query_id = 'nonexistent' SYNC" &>/dev/null

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

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
WHERE is_internal = 1 AND user = '${TEST_USER}' AND current_database = currentDatabase()"

$CLICKHOUSE_CLIENT --query "DROP USER ${TEST_USER}"
