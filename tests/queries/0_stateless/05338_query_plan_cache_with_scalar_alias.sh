#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# The query plan cache is a single, server-wide cache that this test inspects via the global
# `SYSTEM DROP QUERY PLAN CACHE` and exact `QueryPlanCacheHits` counts (no-parallel). Whether a
# second run hits depends on the exact logical plan, which random settings perturb. The cache only
# engages with the analyzer and is incompatible with parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A scalar subquery bound to a `WITH <expression> AS <alias>` element is folded into a constant, so
# the table it reads must be a dependency of the cached plan and must pass the scalar-subquery gate.

$CLICKHOUSE_CLIENT --query "
    DROP VIEW IF EXISTS v_lim;
    DROP TABLE IF EXISTS t_base;
    DROP TABLE IF EXISTS t_limits;

    CREATE TABLE t_base (x Int64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_limits (v Int64) ENGINE = MergeTree ORDER BY v;
    INSERT INTO t_base VALUES (10), (20), (30);
    INSERT INTO t_limits VALUES (15);

    CREATE VIEW v_lim AS WITH (SELECT max(v) FROM t_limits) AS lim SELECT count() AS c FROM t_base WHERE x > lim;
"

# Number of plan cache hits recorded for the most recent run of a query starting with $1.
hits_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query LIKE '$1%'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

CACHE_SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

echo "-- a view whose body has a scalar WITH alias is not cached without the opt-in"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
VIEW_QUERY="SELECT c FROM v_lim"
$CLICKHOUSE_CLIENT $CACHE_SETTINGS --query "$VIEW_QUERY"
$CLICKHOUSE_CLIENT $CACHE_SETTINGS --query "$VIEW_QUERY"
echo "-- hits: $(hits_of_last_run 'SELECT c FROM v_lim')"

echo "-- with the opt-in, the table of a scalar WITH alias is a dependency of the cached plan"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
QUERY="WITH (SELECT max(v) FROM t_limits) AS lim SELECT count() FROM t_base WHERE x > lim"
$CLICKHOUSE_CLIENT $CACHE_SETTINGS --query_plan_cache_allow_scalar_subqueries=1 --query "$QUERY"
$CLICKHOUSE_CLIENT $CACHE_SETTINGS --query_plan_cache_allow_scalar_subqueries=1 --query "$QUERY"
echo "-- hits (warm): $(hits_of_last_run 'WITH (SELECT max(v) FROM t_limits) AS lim')"

# Replacing the table changes its UUID, so the entry must be invalidated and the new value used.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_limits;
    CREATE TABLE t_limits (v Int64) ENGINE = MergeTree ORDER BY v;
    INSERT INTO t_limits VALUES (25);
"
$CLICKHOUSE_CLIENT $CACHE_SETTINGS --query_plan_cache_allow_scalar_subqueries=1 --query "$QUERY"
echo "-- hits after the table was replaced (must be 0): $(hits_of_last_run 'WITH (SELECT max(v) FROM t_limits) AS lim')"

$CLICKHOUSE_CLIENT --query "
    DROP VIEW v_lim;
    DROP TABLE t_base;
    DROP TABLE t_limits;
"
