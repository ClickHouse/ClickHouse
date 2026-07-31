#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-ordinary-database, no-replicated-database
# Same storage-identity race as 04655, but through a view: the view body is expanded into the
# cacheable logical plan at plan time, so the underlying table is analyzed (and must be recorded)
# in a context *copied* from the planning context (`InterpreterSelectQueryAnalyzer::buildContext`,
# the view's SQL-security context). If those copies dropped the identity collector, the plan built
# against the old table (with its row policy baked in as a `FilterStep`) would be stored
# fingerprinting the new table, and re-running the identical query would hit the stale entry.
# See 04655 for the rationale of the failpoint, the asynchronous drop and the tags.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

policy="policy_04656_${CLICKHOUSE_DATABASE}"

# The failpoint is server-wide: if this test ever leaves it enabled, every other query that builds a
# cacheable logical plan on the same server pauses forever. Disable it on every exit path.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_after_logical_plan" 2>/dev/null' EXIT

$CLICKHOUSE_CLIENT --query "
    DROP VIEW IF EXISTS v;
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t VALUES (1), (2), (3);
    CREATE VIEW v AS SELECT a FROM t;
    CREATE ROW POLICY $policy ON ${CLICKHOUSE_DATABASE}.t USING a = 1 TO ALL;
    SYSTEM DROP QUERY PLAN CACHE;
"

$CLICKHOUSE_CLIENT --query "SELECT 'with the row policy:', sum(a) FROM v"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT query_plan_cache_pause_after_logical_plan"

# The plan for this query expands the view body while the row policy is in force, then the query
# blocks in the failpoint until the underlying table has been replaced below.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --query "SELECT 'after the table was replaced:', sum(a) FROM v" > "${CLICKHOUSE_TMP}/04656_result.txt" 2>&1 &
select_pid=$!

# Wait until the query is actually paused inside the plan cache miss path. The pattern anchors at the
# start of the query so that this polling query does not match itself.
for _ in {1..600}
do
    [[ $($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.processes
        WHERE current_database = currentDatabase() AND query LIKE 'SELECT \'after the table was replaced%'") -gt 0 ]] && break
    sleep 0.1
done

# The paused query still holds the old table, so the drop must not wait for the table to be removed
# for real (see 04655). The view is untouched: it references the table by name and now resolves to
# the replacement.
$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously 0 --query "
    DROP ROW POLICY $policy ON ${CLICKHOUSE_DATABASE}.t;
    DROP TABLE t;
    CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t VALUES (1), (2), (3);
"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_after_logical_plan"

wait $select_pid
cat "${CLICKHOUSE_TMP}/04656_result.txt"
rm -f "${CLICKHOUSE_TMP}/04656_result.txt"

# Re-running the *identical* query probes the cache under the same key: had the plan built against
# the replaced table been stored, this would be a hit that executes the new table with the old
# table's baked row policy, returning 1. It must be a miss that plans the current table from scratch.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --query "SELECT 'after the table was replaced:', sum(a) FROM v"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT 'hits and misses of the rerun:', ProfileEvents['QueryPlanCacheHits'], ProfileEvents['QueryPlanCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT \'after the table was replaced%'
    ORDER BY event_time_microseconds DESC LIMIT 1"

$CLICKHOUSE_CLIENT --query "DROP VIEW v; DROP TABLE t"
