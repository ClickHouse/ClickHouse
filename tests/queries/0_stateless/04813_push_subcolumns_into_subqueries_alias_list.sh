#!/usr/bin/env bash
# Tags: no-parallel-replicas, no-random-settings
# The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.
# EXPLAIN is called directly (not as a subquery) because the `(SELECT ...) AS s(x)` alias list
# is used in the queries, and it is lost when the query is converted back to AST for `FROM (EXPLAIN ...)`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --enable_analyzer=1 --optimize_push_subcolumns_into_subqueries=1 --allow_suspicious_types_in_order_by=1"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_push_subcolumns_alias;
CREATE TABLE t_push_subcolumns_alias (id UInt32, json JSON, tup Tuple(a UInt32, b String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_push_subcolumns_alias VALUES (1, '{\"a\": 1, \"b\": \"x\"}', (1, 'one')), (2, '{\"a\": 2, \"b\": \"y\"}', (2, 'two'));
"

echo "aliased subquery"
$CLIENT -q "EXPLAIN actions = 1 SELECT x.a FROM (SELECT json FROM t_push_subcolumns_alias) AS s(x)" | grep -F 'Output' | sed 's/^ *//'
$CLIENT -q "SELECT x.a FROM (SELECT json FROM t_push_subcolumns_alias) AS s(x) ORDER BY x.a"
$CLIENT -q "SELECT x.a FROM (SELECT json FROM t_push_subcolumns_alias) AS s(x) ORDER BY x.a SETTINGS optimize_push_subcolumns_into_subqueries = 0"

echo "aliased subquery, several columns"
$CLIENT -q "EXPLAIN actions = 1 SELECT i, x.a, x.b FROM (SELECT id, tup FROM t_push_subcolumns_alias) AS s(i, x)" | grep -F 'Output' | sed 's/^ *//'
$CLIENT -q "SELECT i, x.a, x.b FROM (SELECT id, tup FROM t_push_subcolumns_alias) AS s(i, x) ORDER BY i"

echo "aliased subquery, whole column and subcolumn"
$CLIENT -q "EXPLAIN actions = 1 SELECT x, x.a FROM (SELECT json FROM t_push_subcolumns_alias) AS s(x)" | grep -F 'Output' | sed 's/^ *//'
$CLIENT -q "SELECT x, x.a FROM (SELECT json FROM t_push_subcolumns_alias) AS s(x) ORDER BY x.a"

echo "aliased subquery over aliased subquery"
$CLIENT -q "EXPLAIN actions = 1 SELECT y.a FROM (SELECT x FROM (SELECT tup FROM t_push_subcolumns_alias) AS s1(x)) AS s2(y)" | grep -F 'Output' | sed 's/^ *//'
$CLIENT -q "SELECT y.a FROM (SELECT x FROM (SELECT tup FROM t_push_subcolumns_alias) AS s1(x)) AS s2(y) ORDER BY y.a"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_push_subcolumns_alias"
