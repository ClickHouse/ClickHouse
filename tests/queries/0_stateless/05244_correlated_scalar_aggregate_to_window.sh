#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./correlated_scalar_aggregate_to_window.lib
. "$CUR_DIR"/correlated_scalar_aggregate_to_window.lib

# `optimize_correlated_scalar_aggregate_to_window` must not change any answer. The shapes of the enclosing query are in
# `05407_correlated_scalar_aggregate_to_window_shapes`, the cases where the subquery is not rewritten are in
# `05385_correlated_scalar_aggregate_to_window_not_rewritten` and `05408_correlated_scalar_aggregate_to_window_not_rewritten_joins`.

create_tables t u x dq nn nk fk u64

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
