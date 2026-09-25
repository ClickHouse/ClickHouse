#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-old-analyzer, no-parallel-replicas, no-ordinary-database, no-replicated-database
# Regression test for a view-definition race on the plan cache hit path. An expanded view has no
# `ReadFromTable` leaf: its definition is inlined into the cached plan, while the leaves of its
# underlying tables still resolve successfully. Therefore the post-`resolveStorages` validation
# must reject the hit when `ALTER VIEW ... MODIFY QUERY` changes the inlined definition.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_after_validation" 2>/dev/null' EXIT

QUERY="SELECT 'value from view:', x FROM v"

$CLICKHOUSE_CLIENT --query "
    DROP VIEW IF EXISTS v;
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a UInt64) ENGINE = Memory;
    INSERT INTO t VALUES (1);
    CREATE VIEW v AS SELECT a AS x FROM t;
    SYSTEM DROP QUERY PLAN CACHE;
"

# Store a plan with the original, inlined view body.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query "$QUERY"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT query_plan_cache_pause_after_validation"

# The second run validates the old view then stops before resolving the base-table leaf.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --query "$QUERY" > "${CLICKHOUSE_TMP}/04905_result.txt" 2>&1 &
select_pid=$!

for _ in {1..600}
do
    [[ $($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.processes
        WHERE current_database = currentDatabase() AND query LIKE 'SELECT \'value from view:%'") -gt 0 ]] && break
    sleep 0.1
done

# Resolving `t` still succeeds, but `v` has no leaf to pin. The post-resolution validation must
# notice the changed view metadata and fall back to normal planning with this new definition.
# `StorageView` does not support `MODIFY QUERY`, so replace the view definition in place instead.
$CLICKHOUSE_CLIENT --query "CREATE OR REPLACE VIEW v AS SELECT a + 100 AS x FROM t"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_after_validation"

wait $select_pid
cat "${CLICKHOUSE_TMP}/04905_result.txt"
rm -f "${CLICKHOUSE_TMP}/04905_result.txt"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT 'hits and stale misses of the raced run:', ProfileEvents['QueryPlanCacheHits'], ProfileEvents['QueryPlanCacheStaleMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT \'value from view:%'
    ORDER BY event_time_microseconds DESC LIMIT 1"

$CLICKHOUSE_CLIENT --query "DROP VIEW v; DROP TABLE t"
