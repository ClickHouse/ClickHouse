#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `optimize_correlated_scalar_aggregate_to_window` must not change any answer. Each case prints whether
# the subquery was rewritten into a window function.

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t (k Int32, v Float64, al Float64 ALIAS v * 2) ENGINE = MergeTree ORDER BY k;
CREATE TABLE u (k Int32, w Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE x (w Int32, z Int32) ENGINE = MergeTree ORDER BY w;
CREATE TABLE u64 (k Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE dq (k Int32, v Decimal(12, 2)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE nn (k Nullable(Int32), v Nullable(Float64)) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
CREATE TABLE nk (k Nullable(Int32), w Int32) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
CREATE TABLE r (k Int32, v Float64) ENGINE = ReplacingMergeTree ORDER BY (k, v);
CREATE TABLE fk (k Float64, v Int32) ENGINE = MergeTree ORDER BY k;

INSERT INTO t VALUES (1, 10), (1, 30), (2, 5), (2, 1e308), (3, -7);
INSERT INTO u VALUES (1, 100), (1, 101), (2, 200), (4, 400);
INSERT INTO x VALUES (100, 1), (101, 2), (101, 3), (200, 4);
INSERT INTO u64 VALUES (1), (2);
INSERT INTO dq VALUES (1, 10.25), (1, 30.75), (2, 5.00);
INSERT INTO nn VALUES (1, 10), (1, NULL), (2, NULL), (NULL, 5), (NULL, 7);
INSERT INTO nk VALUES (1, 1), (2, 2), (NULL, 3);
INSERT INTO r VALUES (1, 10), (1, 30), (2, 5);
INSERT INTO fk VALUES (0, 1), (-0, 2), (nan, 3), (nan, 4), (1.5, 5);
"

function check()
{
    local name="$1"
    local query="$2"
    local off on fired
    off=$($CLICKHOUSE_CLIENT --optimize_correlated_scalar_aggregate_to_window=0 -q "$query")
    on=$($CLICKHOUSE_CLIENT --optimize_correlated_scalar_aggregate_to_window=1 -q "$query")
    fired=$($CLICKHOUSE_CLIENT --optimize_correlated_scalar_aggregate_to_window=1 \
        -q "SELECT count() FROM (EXPLAIN $query) WHERE explain LIKE '%Window step for window%'")
    echo "=== $name: rewritten $fired ==="
    echo "$on"
    if [ "$off" != "$on" ]; then
        echo "DIFFERENT FROM THE RESULT WITHOUT THE REWRITE:"
        echo "$off"
    fi
}

SQ="(SELECT max(v) FROM t AS s WHERE s.k = u.k)"

echo "--- aggregates ---"
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

echo "--- shapes of the enclosing query ---"
check "TPC-H Q17 shape" "SELECT sum(t.v) FROM u, t WHERE u.k = t.k AND u.w >= 100 AND t.v < (SELECT 0.5 * avg(v) FROM t AS s WHERE s.k = u.k)"
check "in WHERE with rows kept" "SELECT u.w AS w, t.v AS v FROM u, t WHERE u.k = t.k AND u.w > 100 AND t.v >= (SELECT 0.5 * max(v) FROM t AS s WHERE s.k = u.k) ORDER BY w, v"
check "two subqueries in one WHERE" "SELECT u.w AS w, t.v AS v FROM u, t WHERE u.k = t.k AND u.w > 100 AND t.v > (SELECT min(v) FROM t AS s WHERE s.k = u.k) AND t.v <= (SELECT max(v) FROM t AS s WHERE s.k = u.k) ORDER BY w, v"
check "correlated to the same table" "SELECT o.k AS k, o.v AS v, (SELECT count() FROM t AS s WHERE s.k = o.k) AS c FROM t AS o WHERE o.k > 1 ORDER BY k, v"
check "condition on the correlated table" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE t.v > 5 AND u.w > 0 ORDER BY k, v"
check "condition on an ALIAS column of the correlated table" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE t.al < 100 AND u.w > 0 ORDER BY k, v"
check "join condition on a non-key column" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k AND t.v > u.w ORDER BY k, v"
check "ANY join" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u ANY INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "LEFT join, correlated table on the left" "SELECT o.k AS k, o.v AS v, u.w AS w, (SELECT max(v) FROM t AS s WHERE s.k = o.k) AS c FROM t AS o LEFT JOIN u ON u.k = o.k WHERE o.k > 0 ORDER BY k, v, w"
check "third table joined to the other relation" "SELECT u.k AS k, x.z AS z, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k INNER JOIN x ON u.w = x.w WHERE x.z > 1 ORDER BY k, z, v"
check "join_use_nulls" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v SETTINGS join_use_nulls = 1"
check "LIMIT" "SELECT c = multiIf(k = 1, 30, k = 2, 1e308, nan), v > 0 FROM (SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 LIMIT 1)"
check "IN subquery in WHERE" "SELECT count() FROM u, t WHERE u.k = t.k AND u.w IN (SELECT w FROM x) AND t.v >= (SELECT max(v) FROM t AS s WHERE s.k = u.k)"
check "non-deterministic condition" "SELECT countIf(c != multiIf(k = 1, 30, k = 2, 1e308, nan) OR v < -100) FROM (SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE rand() % 2 = 0)"
check "inside a correlated subquery" "SELECT o, (SELECT max(t.v) FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 100 AND z.o = 1 AND t.v >= (SELECT max(v) FROM t AS s WHERE s.k = u.k)) AS c FROM (SELECT number + 1 AS o FROM numbers(3)) AS z ORDER BY o"
check "FINAL on both" "SELECT u.k AS k, r.v AS v, (SELECT max(v) FROM r AS s FINAL WHERE s.k = u.k) AS c FROM u INNER JOIN r FINAL ON u.k = r.k WHERE u.w > 0 ORDER BY k, v"
check "additional_table_filters" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v SETTINGS additional_table_filters = {'t': 'v < 20'}"

echo "--- keys ---"
check "Nullable key and argument" "SELECT nk.k AS k, nn.v AS v, (SELECT max(v) FROM nn AS s WHERE s.k = nk.k) AS c FROM nk INNER JOIN nn ON nk.k = nn.k WHERE nk.w > 0 ORDER BY k, v"
check "NULL key, correlated to the same table" "SELECT o.k AS k, o.v AS v, (SELECT count() FROM nn AS s WHERE s.k = o.k) AS c FROM nn AS o WHERE o.k IS NULL OR o.k > 0 ORDER BY k, v"
check "Float key" "SELECT count() FROM (SELECT o.k AS k, o.v AS v, (SELECT count() FROM fk AS s WHERE s.k = o.k) AS c FROM fk AS o WHERE o.k > -1 OR isNaN(o.k))"
check "key types differ" "SELECT u64.k AS k, t.v AS v, (SELECT max(v) FROM t AS s WHERE s.k = u64.k) AS c FROM u64 INNER JOIN t ON u64.k = t.k WHERE u64.k > 0 ORDER BY k, v"

echo "--- not rewritten ---"
check "nothing restricts the keys, the decorrelated plan needs less memory" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k ORDER BY k, v"
check "correlated table on the NULL-extended side" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u LEFT JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "RIGHT join" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u RIGHT JOIN t ON u.k = t.k WHERE t.v > 0 ORDER BY k, v"
check "non-equality correlation" "SELECT u.k AS k, t.v AS v, (SELECT max(v) FROM t AS s WHERE s.k <= u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "no equality between the keys" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.w = t.k * 100 WHERE u.w > 0 ORDER BY k, v"
check "self-join" "SELECT a.k AS k, a.v AS v, (SELECT max(v) FROM t AS s WHERE s.k = a.k) AS c FROM t AS a INNER JOIN t AS b ON a.k = b.k WHERE a.k > 0 ORDER BY k, v"
check "FINAL only in the subquery" "SELECT u.k AS k, r.v AS v, (SELECT max(v) FROM r AS s FINAL WHERE s.k = u.k) AS c FROM u INNER JOIN r ON u.k = r.k WHERE u.w > 0 ORDER BY k, v"
check "GROUP BY in the subquery" "SELECT u.k AS k, t.v AS v, (SELECT max(v) FROM t AS s WHERE s.k = u.k GROUP BY s.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "arrayJoin in the subquery" "SELECT count(), sum(v), max(k) FROM (SELECT u.k AS k, t.v AS v, (SELECT arrayJoin(arrayFilter(x -> x < 0, [max(v)])) FROM t AS s WHERE s.k = u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0)"
check "non-deterministic function in the subquery" "SELECT u.k AS k, t.v AS v, (SELECT max(v) + rand64() % 1 FROM t AS s WHERE s.k = u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "outer GROUP BY with the subquery in SELECT" "SELECT u.k AS k, $SQ AS c, sum(t.v) FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 GROUP BY u.k ORDER BY k"
check "subqueries correlated to each other's table" "SELECT count(), sum(o.v) FROM u AS p INNER JOIN t AS o ON p.k = o.k WHERE p.w > 0 AND o.v < (SELECT max(v) FROM t AS s WHERE s.k = p.k) AND p.w <= (SELECT max(w) FROM u AS s2 WHERE s2.k = o.k)"
check "-If aggregate with a condition in the subquery" "SELECT u.k AS k, t.v AS v, (SELECT countIf(v > 5) FROM t AS s WHERE s.k = u.k AND s.v > 1000) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "result type that cannot be Nullable" "SELECT u.k AS k, t.v AS v, (SELECT groupArraySorted(3)(v) FROM t AS s WHERE s.k = u.k AND s.v > 20) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "argument that may throw on rows the condition skips" "SELECT u.w AS w, x.z AS z, (SELECT sum(intDiv(10, s.z - 1)) FROM x AS s WHERE s.w = u.w AND s.z != 1) AS c FROM u INNER JOIN x ON u.w = x.w WHERE u.k > 0 ORDER BY w, z"
check "count of a Nullable column" "SELECT nk.k AS k, nn.v AS v, (SELECT count(v) FROM nn AS s WHERE s.k = nk.k) AS c FROM nk INNER JOIN nn ON nk.k = nn.k WHERE nk.w > 0 ORDER BY k, v"
check "outer key inside an expression argument" "SELECT u.k AS k, t.v AS v, (SELECT sum(intDiv(10, u.k)) FROM t AS s WHERE s.k = u.k) AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
