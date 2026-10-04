#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-old-analyzer, no-parallel-replicas, no-ordinary-database, no-replicated-database
# Regression test for a view-definition race on the plan cache miss path, the analogue of `04905`
# for the hit path. An expanded view has no `ReadFromTable` leaf: its definition is inlined into the
# plan, while the leaves of its underlying tables still resolve successfully. If the view is replaced
# after its dependencies were collected but before `resolveStorages`, the plan must be neither
# stored nor executed with the old view body.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The failpoint is server-wide: disable it on every exit path.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_before_resolve_storages" 2>/dev/null' EXIT

QUERY="SELECT 'value from view:', x FROM v"

$CLICKHOUSE_CLIENT --query "
    DROP VIEW IF EXISTS v;
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a UInt64) ENGINE = Memory;
    INSERT INTO t VALUES (1);
    CREATE VIEW v AS SELECT a AS x FROM t;
    SYSTEM DROP QUERY PLAN CACHE;
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT query_plan_cache_pause_before_resolve_storages"

# The plan is built with the original view body, then the query stops before resolving the
# base-table leaf.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --query "$QUERY" > "${CLICKHOUSE_TMP}/05261_result.txt" 2>&1 &
select_pid=$!

for _ in {1..600}
do
    [[ $($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.processes
        WHERE current_database = currentDatabase() AND query LIKE 'SELECT \'value from view:%'") -gt 0 ]] && break
    sleep 0.1
done

# Resolving `t` still succeeds, but `v` has no leaf to pin. The validation after `resolveStorages`
# must notice the changed view and fall back to normal planning with the new definition.
# The paused query still holds the old view, so the replacement must not wait for it to be dropped
# for real: `database_atomic_wait_for_drop_and_detach_synchronously` is enabled in the test
# configuration and would deadlock with the query this test is pausing on purpose (see `04811`).
$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously 0 \
    --query "CREATE OR REPLACE VIEW v AS SELECT a + 100 AS x FROM t"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_before_resolve_storages"

wait $select_pid
cat "${CLICKHOUSE_TMP}/05261_result.txt"
rm -f "${CLICKHOUSE_TMP}/05261_result.txt"

# The stale plan must not have been stored.
$CLICKHOUSE_CLIENT --query "SELECT 'entries after the race:', value FROM system.metrics WHERE metric = 'QueryPlanCacheEntries'"

# The identical query is a miss that plans the current view and stores a live entry; the next one hits it.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query "$QUERY"
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query "$QUERY"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT 'hits and misses:', ProfileEvents['QueryPlanCacheHits'], ProfileEvents['QueryPlanCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT \'value from view:%'
    ORDER BY event_time_microseconds"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DROP QUERY PLAN CACHE;
    DROP VIEW v;
    DROP TABLE t;
"
