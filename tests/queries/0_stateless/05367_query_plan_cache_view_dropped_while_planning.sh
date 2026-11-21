#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-ordinary-database, no-replicated-database
# A view is expanded into the cacheable logical plan, so the plan reads only the underlying table.
# If the view is dropped after the plan was built but before its dependencies are collected, the
# raw AST walk cannot tell the missing view from a CTE name and skips it. The plan must then be
# neither executed nor stored: otherwise the query would succeed against a view that no longer
# exists, and the entry - fingerprinting only the underlying table - would keep serving the old
# view body after a view with the same name but a different body is created.
# See 04655 for the rationale of the failpoint and the tags. The failpoint is server-wide, which
# is why the test is `no-parallel`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_after_logical_plan" 2>/dev/null' EXIT

$CLICKHOUSE_CLIENT --query "
    DROP VIEW IF EXISTS v;
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t VALUES (1), (2), (3);
    CREATE VIEW v AS SELECT a FROM t;
    SYSTEM DROP QUERY PLAN CACHE;
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT query_plan_cache_pause_after_logical_plan"

$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --query "SELECT 'view body:', sum(a) FROM v" > "${CLICKHOUSE_TMP}/05367_result.txt" 2>&1 &
select_pid=$!

# Wait until the query is paused inside the plan cache miss path. The pattern anchors at the start
# of the query so that this polling query does not match itself.
for _ in {1..600}
do
    [[ $($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.processes
        WHERE current_database = currentDatabase() AND query LIKE 'SELECT \'view body:%'") -gt 0 ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously 0 --query "DROP VIEW v"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_after_logical_plan"

wait $select_pid
# The query falls back to the normal interpreter, which analyzes the current catalog.
grep -q 'UNKNOWN_TABLE' "${CLICKHOUSE_TMP}/05367_result.txt" && echo 'UNKNOWN_TABLE'
rm -f "${CLICKHOUSE_TMP}/05367_result.txt"

# A view with the same name but a different body. Had the plan of the old view been stored, the
# identical query would hit it and return 6.
$CLICKHOUSE_CLIENT --query "CREATE VIEW v AS SELECT a * 10 AS a FROM t"
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --query "SELECT 'view body:', sum(a) FROM v"

$CLICKHOUSE_CLIENT --query "DROP VIEW v; DROP TABLE t"
