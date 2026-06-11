#!/usr/bin/env bash
# Tags: no-parallel-replicas
# The query plan cache is incompatible with parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query_plan_cache_allow_scalar_subqueries=1"

# A query over nested views with a join, a subquery and a scalar subquery: the shape the
# complex-query plan cache is built for.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_points;
    DROP TABLE IF EXISTS t_labels;
    DROP VIEW IF EXISTS v_joined;
    DROP VIEW IF EXISTS v_top;

    CREATE TABLE t_points (id UInt64, x Int64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE t_labels (id UInt64, label String) ENGINE = Memory;

    INSERT INTO t_points VALUES (1, 10), (2, 20), (3, 30);
    INSERT INTO t_labels VALUES (1, 'a'), (2, 'b'), (3, 'c');

    CREATE VIEW v_joined AS
        SELECT p.id AS id, p.x AS x, l.label AS label
        FROM t_points AS p
        INNER JOIN t_labels AS l ON p.id = l.id
        WHERE p.id IN (SELECT id FROM t_points WHERE x <= 1000);

    CREATE VIEW v_top AS
        SELECT label, sum(x) AS total, (SELECT max(x) FROM t_points) AS global_max
        FROM v_joined
        GROUP BY label;
"

QUERY="SELECT label, total, global_max FROM v_top ORDER BY label"

run_query()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$QUERY"
}

hits_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query LIKE 'SELECT label, total, global_max%'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

echo "-- first run (cold, miss)"
run_query
echo "-- hits: $(hits_of_last_run)"

echo "-- second run (hit)"
run_query
echo "-- hits: $(hits_of_last_run)"

# Note: `global_max` comes from a scalar subquery whose value is baked into the cached
# plan (the documented trade-off of query_plan_cache_allow_scalar_subqueries=1), so it
# stays 30 here; the joined table reads are fresh and row 'd' appears.
echo "-- data freshness: cache hit must see newly inserted rows"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_points VALUES (4, 40)"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_labels VALUES (4, 'd')"
run_query
echo "-- hits: $(hits_of_last_run)"

echo "-- schema change invalidates: ALTER ADD COLUMN forces re-planning"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_points ADD COLUMN y Int64 DEFAULT 0"
run_query
echo "-- hits: $(hits_of_last_run)"

echo "-- view redefinition invalidates: changed body must be picked up"
$CLICKHOUSE_CLIENT --query "
    CREATE OR REPLACE VIEW v_joined AS
        SELECT p.id AS id, p.x * 2 AS x, l.label AS label
        FROM t_points AS p
        INNER JOIN t_labels AS l ON p.id = l.id
"
run_query
echo "-- hits: $(hits_of_last_run)"

echo "-- scalar subqueries are not cached without the opt-in setting"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query "$QUERY" > /dev/null
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query "$QUERY" > /dev/null
echo "-- hits: $(hits_of_last_run)"

echo "-- non-deterministic functions inside a view body are never cached"
$CLICKHOUSE_CLIENT --query "CREATE VIEW v_nondet AS SELECT id, x, rand() AS r FROM t_points"
NONDET_QUERY="SELECT count() FROM v_nondet WHERE r >= 0"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $SETTINGS --query "$NONDET_QUERY" > /dev/null
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $SETTINGS --query "$NONDET_QUERY" > /dev/null
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT '-- hits: ' || toString(ProfileEvents['QueryPlanCacheHits'])
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND type = 'QueryFinish'
      AND query LIKE 'SELECT count() FROM v_nondet%'
    ORDER BY event_time_microseconds DESC
    LIMIT 1"
$CLICKHOUSE_CLIENT --query "DROP VIEW v_nondet"

$CLICKHOUSE_CLIENT --query "
    DROP VIEW v_top;
    DROP VIEW v_joined;
    DROP TABLE t_points;
    DROP TABLE t_labels;
"
