#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query_plan_cache_allow_scalar_subqueries=1"
QUERY="WITH t AS (SELECT toUInt64(0) AS x) SELECT x FROM v"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t;
    DROP VIEW IF EXISTS v;
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    INSERT INTO t VALUES (1);
    CREATE VIEW v AS SELECT (SELECT max(x) FROM t) AS x;
    SYSTEM DROP QUERY PLAN CACHE;
"

# The outer CTE must not shadow `t` in the stored view body. The scalar subquery
# is cacheable only with the explicit opt-in, and its source table must be recorded
# as a dependency despite having no leaf in the logical plan.
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $SETTINGS --query "$QUERY"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $SETTINGS --query "$QUERY"

$CLICKHOUSE_CLIENT --query "DROP TABLE t; CREATE TABLE t (x UInt64) ENGINE = Memory; INSERT INTO t VALUES (2)"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $SETTINGS --query "$QUERY"

$CLICKHOUSE_CLIENT --query "DROP VIEW v; DROP TABLE t"
