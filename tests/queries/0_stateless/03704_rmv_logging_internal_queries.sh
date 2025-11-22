#!/usr/bin/env bash
# Tags: atomic-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that some internal queries from refreshable materialized views are logged correctly

$CLICKHOUSE_CLIENT --query "CREATE VIEW one_proxy AS SELECT * FROM system.one"
$CLICKHOUSE_CLIENT --query "
CREATE MATERIALIZED VIEW rmv_test
REFRESH EVERY 1 HOUR
(
    dummy UInt8
)
ENGINE = MergeTree
ORDER BY dummy
EMPTY
AS SELECT
    dummy
FROM one_proxy"
$CLICKHOUSE_CLIENT --query "SYSTEM REFRESH VIEW rmv_test"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT VIEW rmv_test"

# refresh with the source table absent to verify that exceptions are logged, too
$CLICKHOUSE_CLIENT --query "DROP VIEW one_proxy"
$CLICKHOUSE_CLIENT --query "SYSTEM REFRESH VIEW rmv_test"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT VIEW rmv_test" 2> /dev/null

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
SELECT
    countIf(query LIKE '%INSERT INTO $CLICKHOUSE_DATABASE.\`.tmp.inner_id.%' AND type = 'QueryStart'),
    countIf(query LIKE '%INSERT INTO $CLICKHOUSE_DATABASE.\`.tmp.inner_id.%' AND type = 'QueryFinish'),
    countIf(query = '(create target table)' AND type = 'ExceptionBeforeStart') > 0
FROM system.query_log
WHERE is_internal = 1 AND current_database IN [currentDatabase(), 'default']
"

$CLICKHOUSE_CLIENT --query "DROP VIEW rmv_test"
