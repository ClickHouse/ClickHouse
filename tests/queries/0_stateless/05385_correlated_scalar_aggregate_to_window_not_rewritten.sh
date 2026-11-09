#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./correlated_scalar_aggregate_to_window.lib
. "$CUR_DIR"/correlated_scalar_aggregate_to_window.lib

# `optimize_correlated_scalar_aggregate_to_window` must not change any answer, also where the subquery is not rewritten.
# The joins that leave the subquery not rewritten are in `05408_correlated_scalar_aggregate_to_window_not_rewritten_joins`.

create_tables t u x nn nk r tn vt tp

check "nothing restricts the keys, the decorrelated plan needs less memory" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k ORDER BY k, v"
check "non-deterministic condition on the key, not pushed below the window" "SELECT countIf(c != multiIf(k = 1, 30, k = 2, 1e308, -7)) FROM (SELECT o.k AS k, (SELECT max(v) FROM t AS s WHERE s.k = o.k) AS c FROM t AS o WHERE rand64(o.k) % 2 = 0)"
check "non-deterministic condition on the joined table, not pushed below the join" "SELECT countIf(c != multiIf(k = 1, 30, k = 2, 1e308, nan) OR v < -100) FROM (SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE rand(u.w) % 2 = 0)"
check "correlated table on the NULL-extended side" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u LEFT JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "RIGHT join" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u RIGHT JOIN t ON u.k = t.k WHERE t.v > 0 ORDER BY k, v"
check "non-equality correlation" "SELECT u.k AS k, t.v AS v, (SELECT max(v) FROM t AS s WHERE s.k <= u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "no equality between the keys" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.w = t.k * 100 WHERE u.w > 0 ORDER BY k, v"
check "FINAL only in the subquery" "SELECT u.k AS k, r.v AS v, (SELECT max(v) FROM r AS s FINAL WHERE s.k = u.k) AS c FROM u INNER JOIN r ON u.k = r.k WHERE u.w > 0 ORDER BY k, v"
check "GROUP BY in the subquery" "SELECT u.k AS k, t.v AS v, (SELECT max(v) FROM t AS s WHERE s.k = u.k GROUP BY s.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "arrayJoin in the subquery" "SELECT count(), sum(v), max(k) FROM (SELECT u.k AS k, t.v AS v, (SELECT arrayJoin(arrayFilter(x -> x < 0, [max(v)])) FROM t AS s WHERE s.k = u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0)"
check "non-deterministic function in the subquery" "SELECT u.k AS k, t.v AS v, (SELECT max(v) + rand64() % 1 FROM t AS s WHERE s.k = u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "outer GROUP BY with the subquery in SELECT" "SELECT u.k AS k, $SQ AS c, sum(t.v) FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 GROUP BY u.k ORDER BY k"
check "subqueries correlated to each other's table" "SELECT count(), sum(o.v) FROM u AS p INNER JOIN t AS o ON p.k = o.k WHERE p.w > 0 AND o.v < (SELECT max(v) FROM t AS s WHERE s.k = p.k) AND p.w <= (SELECT max(w) FROM u AS s2 WHERE s2.k = o.k)"
check "subqueries over each other's table, keyed by the other table" "SELECT u.k AS k, t.v AS v, (SELECT max(v) FROM t AS s WHERE s.k = u.k) AS a, (SELECT max(w) FROM u AS s2 WHERE s2.k = t.k) AS b FROM u INNER JOIN t ON u.k = t.k WHERE u.k > 0 AND t.k > 0 ORDER BY k, v"
check "-If aggregate with a condition in the subquery" "SELECT u.k AS k, t.v AS v, (SELECT countIf(v > 5) FROM t AS s WHERE s.k = u.k AND s.v > 1000) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "result type that cannot be Nullable" "SELECT u.k AS k, t.v AS v, (SELECT groupArraySorted(3)(v) FROM t AS s WHERE s.k = u.k AND s.v > 20) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "argument that may throw on rows the condition skips" "SELECT u.w AS w, x.z AS z, (SELECT sum(intDiv(10, s.z - 1)) FROM x AS s WHERE s.w = u.w AND s.z != 1) AS c FROM u INNER JOIN x ON u.w = x.w WHERE u.k > 0 ORDER BY w, z"
check "count of a Nullable column" "SELECT nk.k AS k, nn.v AS v, (SELECT count(v) FROM nn AS s WHERE s.k = nk.k) AS c FROM nk INNER JOIN nn ON nk.k = nn.k WHERE nk.w > 0 ORDER BY k, v"
check "outer key inside an expression argument" "SELECT u.k AS k, t.v AS v, (SELECT sum(intDiv(10, u.k)) FROM t AS s WHERE s.k = u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "NULL inside a Tuple key" "SELECT toString(o.k) AS k, o.v AS v, (SELECT count() FROM tn AS s WHERE s.k = o.k) AS c FROM tn AS o WHERE toString(o.k) != '' ORDER BY v"
check "NULL in a Variant key" "SELECT o.v AS v, (SELECT count() FROM vt AS s WHERE s.k = o.k) AS c FROM vt AS o WHERE toString(variantType(o.k)) != 'x' ORDER BY v"
check "expression over no matching rows without short-circuit" "SELECT u.k AS k, t.v AS v, (SELECT intDiv(10, count()) FROM t AS s WHERE s.k = u.k AND s.v > 100) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v SETTINGS short_circuit_function_evaluation = 'disable'"
check "no filter push-down" "SELECT o.k AS k, o.v AS v, (SELECT count() FROM t AS s WHERE s.k = o.k) AS c FROM t AS o WHERE o.k > 1 ORDER BY k, v SETTINGS query_plan_filter_push_down = 0"
check "conditions on the keys of different windows" "SELECT o.id AS id, (SELECT max(v) FROM tp AS s WHERE s.k = o.k) AS a, (SELECT max(v) FROM tp AS s WHERE s.id = o.id) AS b FROM tp AS o WHERE o.k = 1 AND o.id = 2"
check "aggregate with parameters over only NULLs" "SELECT o.k AS k, (SELECT uniqUpTo(5)(NULL) FROM t AS s WHERE s.k = o.k) AS c FROM t AS o WHERE o.k > 0 ORDER BY k"

run_all
