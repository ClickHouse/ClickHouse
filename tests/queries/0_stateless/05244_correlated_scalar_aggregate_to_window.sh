#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./correlated_scalar_aggregate_to_window.lib
. "$CUR_DIR"/correlated_scalar_aggregate_to_window.lib

# `optimize_correlated_scalar_aggregate_to_window` must not change any answer. The cases where the subquery is
# not rewritten are in `05385_correlated_scalar_aggregate_to_window_not_rewritten`.

create_tables t u x u64 dq nn nk r fk ts tc

section "--- aggregates ---"
check "max" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "min" "SELECT u.k AS k, t.v AS v, (SELECT min(v) FROM t AS s WHERE s.k = u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "avg over Float64, the join repeats rows" "SELECT u.k AS k, t.v AS v, (SELECT avg(v) FROM t AS s WHERE s.k = u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "avg over Decimal" "SELECT u.k AS k, dq.v AS v, (SELECT avg(v) FROM dq AS s WHERE s.k = u.k) AS c FROM u INNER JOIN dq ON u.k = dq.k WHERE u.w > 0 ORDER BY k, v"
check "sum" "SELECT u.k AS k, t.v AS v, (SELECT sum(v) FROM t AS s WHERE s.k = u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "count" "SELECT u.k AS k, t.v AS v, (SELECT count() FROM t AS s WHERE s.k = u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "two aggregates and parameters" "SELECT u.k AS k, t.v AS v, (SELECT max(v) - quantileExact(0.5)(v) + uniqExact(v) FROM t AS s WHERE s.k = u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "maxIf" "SELECT u.k AS k, t.v AS v, (SELECT maxIf(v, v < 20) FROM t AS s WHERE s.k = u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "expression argument" "SELECT u.k AS k, t.v AS v, (SELECT max(v * v) FROM t AS s WHERE s.k = u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "aggregate_functions_null_for_empty" "SELECT u.k AS k, t.v AS v, (SELECT sum(v) FROM t AS s WHERE s.k = u.k AND s.v > 1000) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v SETTINGS aggregate_functions_null_for_empty = 1"
check "extra condition in the subquery" "SELECT u.k AS k, t.v AS v, (SELECT max(v) FROM t AS s WHERE s.k = u.k AND s.v < 20) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "no row matches the condition of the subquery" "SELECT u.k AS k, t.v AS v, (SELECT count() FROM t AS s WHERE s.k = u.k AND s.v > 20) AS c, (SELECT max(v) FROM t AS s WHERE s.k = u.k AND s.v > 1000) AS m FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "outer key in the argument" "SELECT u.k AS k, t.v AS v, (SELECT sum(v + u.k) FROM t AS s WHERE s.k = u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "condition of a non-boolean type" "SELECT u.w AS w, x.z AS z, (SELECT max(z) FROM x AS s WHERE s.w = u.w AND s.z) AS c FROM u INNER JOIN x ON u.w = x.w WHERE u.k > 0 ORDER BY w, z"
check "two subqueries" "SELECT u.k AS k, t.v AS v, (SELECT min(v) FROM t AS s WHERE s.k = u.k) AS a, (SELECT count() FROM t AS s WHERE s.k = u.k) AS b FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"

section "--- shapes of the enclosing query ---"
check "ORDER BY ALL shares the expression with the projection" "SELECT u.k AS k, t.v AS v, $SQ + 1 FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY ALL"
check "TPC-H Q17 shape" "SELECT sum(t.v) FROM u, t WHERE u.k = t.k AND u.w >= 100 AND t.v < (SELECT 0.5 * avg(v) FROM t AS s WHERE s.k = u.k)"
check "in WHERE with rows kept" "SELECT u.w AS w, t.v AS v FROM u, t WHERE u.k = t.k AND u.w > 100 AND t.v >= (SELECT 0.5 * max(v) FROM t AS s WHERE s.k = u.k) ORDER BY w, v"
check "two subqueries in one WHERE" "SELECT u.w AS w, t.v AS v FROM u, t WHERE u.k = t.k AND u.w > 100 AND t.v > (SELECT min(v) FROM t AS s WHERE s.k = u.k) AND t.v <= (SELECT max(v) FROM t AS s WHERE s.k = u.k) ORDER BY w, v"
check "correlated to the same table" "SELECT o.k AS k, o.v AS v, (SELECT count() FROM t AS s WHERE s.k = o.k) AS c FROM t AS o WHERE o.k > 1 ORDER BY k, v"
check "condition on the correlated table" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE t.v > 5 AND u.w > 0 ORDER BY k, v"
check "condition on an ALIAS column of the correlated table" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE t.al < 100 AND u.w > 0 ORDER BY k, v"
check "join condition on a non-key column" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k AND t.v > u.w ORDER BY k, v"
# Which of the matching rows `ANY` keeps is not defined, so `t.v` is not selected; `c` is the same for all of them.
check "ANY join" "SELECT u.k AS k, $SQ AS c FROM u ANY INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k"
check "LEFT join, correlated table on the left" "SELECT o.k AS k, o.v AS v, u.w AS w, (SELECT max(v) FROM t AS s WHERE s.k = o.k) AS c FROM t AS o LEFT JOIN u ON u.k = o.k WHERE o.k > 0 ORDER BY k, v, w"
check "third table joined to the other relation" "SELECT u.k AS k, x.z AS z, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k INNER JOIN x ON u.w = x.w WHERE x.z > 1 ORDER BY k, z, v"
check "join_use_nulls" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v SETTINGS join_use_nulls = 1"
check "LIMIT" "SELECT c = multiIf(k = 1, 30, k = 2, 1e308, nan), v > 0 FROM (SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 LIMIT 1)"
check "IN subquery in WHERE" "SELECT count() FROM u, t WHERE u.k = t.k AND u.w IN (SELECT w FROM x) AND t.v >= (SELECT max(v) FROM t AS s WHERE s.k = u.k)"
check "non-deterministic condition" "SELECT countIf(c != multiIf(k = 1, 30, k = 2, 1e308, nan) OR v < -100) FROM (SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 AND rand(u.w) % 2 = 0)"
check "inside a correlated subquery" "SELECT o, (SELECT max(t.v) FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 100 AND z.o = 1 AND t.v >= (SELECT max(v) FROM t AS s WHERE s.k = u.k)) AS c FROM (SELECT number + 1 AS o FROM numbers(3)) AS z ORDER BY o"
check "FINAL on both" "SELECT u.k AS k, r.v AS v, (SELECT max(v) FROM r AS s FINAL WHERE s.k = u.k) AS c FROM u INNER JOIN r FINAL ON u.k = r.k WHERE u.w > 0 ORDER BY k, v"
check "additional_table_filters" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v SETTINGS additional_table_filters = {'t': 'v < 20'}"
check "self-join" "SELECT a.k AS k, a.v AS v, (SELECT max(v) FROM t AS s WHERE s.k = a.k) AS c FROM t AS a INNER JOIN t AS b ON a.k = b.k WHERE a.k > 0 ORDER BY k, v"
check "column named like a window column" "SELECT o.k AS k, o.__correlated_aggregate_0 AS a, (SELECT max(__correlated_aggregate_0) FROM tc AS s WHERE s.k = o.k) AS c FROM tc AS o WHERE o.k > 0 ORDER BY k, a"
check "subcolumn" "SELECT o.k AS k, o.s AS s FROM ts AS o WHERE o.k > 0 AND length(o.s) * 2 < (SELECT sum(length(s)) FROM ts AS z WHERE z.k = o.k) ORDER BY k, s"
# The window reads the subcolumn `s.size` instead of the whole column. The plan is checked without parallel replicas,
# which describe the window step differently.
section "$($CLICKHOUSE_CLIENT --optimize_correlated_scalar_aggregate_to_window=1 --enable_parallel_replicas=0 --optimize_functions_to_subcolumns=1 -q "
    SELECT 'subcolumn: rewritten', countIf(explain LIKE '%Window step for window%'), 'reads the whole column', countIf(explain LIKE '%s String%') > 0
    FROM (EXPLAIN header = 1, actions = 1
    SELECT o.k AS k FROM ts AS o WHERE o.k > 0 AND length(o.s) * 2 < (SELECT sum(length(s)) FROM ts AS z WHERE z.k = o.k))")"

section "--- keys ---"
check "Nullable key and argument" "SELECT nk.k AS k, nn.v AS v, (SELECT max(v) FROM nn AS s WHERE s.k = nk.k) AS c FROM nk INNER JOIN nn ON nk.k = nn.k WHERE nk.w > 0 ORDER BY k, v"
check "NULL key, correlated to the same table" "SELECT o.k AS k, o.v AS v, (SELECT count() FROM nn AS s WHERE s.k = o.k) AS c FROM nn AS o WHERE o.k IS NULL OR o.k > 0 ORDER BY k, v"
# `intDiv` throws only for the row with a NULL key and `v = 5`, which the subquery skips.
check "NULL key and an argument that may throw" "SELECT o.k AS k, o.v AS v, (SELECT sum(intDiv(10, toInt32(s.v) - 5)) FROM nn AS s WHERE s.k = o.k) AS c FROM nn AS o WHERE o.k IS NULL OR o.k > 0 ORDER BY k, v"
# Without short-circuit, `and` evaluates the condition also for the row with a NULL key. Only the rewrite is checked:
# the query without the rewrite does not guarantee it either (e.g. with `PREWHERE`).
section "$($CLICKHOUSE_CLIENT --optimize_correlated_scalar_aggregate_to_window=1 --short_circuit_function_evaluation=disable -q "
    SELECT 'NULL key and a condition that may throw without short-circuit: rewritten', countIf(explain LIKE '%function_type: window%') > 0 FROM (EXPLAIN QUERY TREE
    SELECT o.k AS k, (SELECT count() FROM nn AS s WHERE s.k = o.k AND intDiv(10, toInt32(s.v) - 5) > 0) AS c FROM nn AS o WHERE o.k IS NULL OR o.k > 0)")"
check "Float key" "SELECT count() FROM (SELECT o.k AS k, o.v AS v, (SELECT count() FROM fk AS s WHERE s.k = o.k) AS c FROM fk AS o WHERE o.k > -1 OR isNaN(o.k))"
check "key types differ" "SELECT u64.k AS k, t.v AS v, (SELECT max(v) FROM t AS s WHERE s.k = u64.k) AS c FROM u64 INNER JOIN t ON u64.k = t.k WHERE u64.k > 0 ORDER BY k, v"

run_all
