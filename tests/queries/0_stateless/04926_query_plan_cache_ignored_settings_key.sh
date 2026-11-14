#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t VALUES (1), (2), (3);
"

run()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$1"
}

hits_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query = '$1'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

PLAIN_QUERY="SELECT sum(k) FROM t"
VALUE_QUERY="SELECT sum(k) FROM t SETTINGS max_threads = 1"
DEFAULT_QUERY="SELECT sum(k) FROM t SETTINGS max_threads = DEFAULT"

echo "-- ignored setting value shares the entry with no SETTINGS clause"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
run "$PLAIN_QUERY"
run "$VALUE_QUERY"
echo "-- hits: $(hits_of_last_run "$VALUE_QUERY")"

echo "-- ignored setting DEFAULT shares the entry with no SETTINGS clause"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
run "$PLAIN_QUERY"
run "$DEFAULT_QUERY"
echo "-- hits: $(hits_of_last_run "$DEFAULT_QUERY")"

$CLICKHOUSE_CLIENT --query "DROP TABLE t"
