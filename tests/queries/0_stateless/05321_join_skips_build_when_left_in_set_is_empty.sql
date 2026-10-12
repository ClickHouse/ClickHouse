-- Tags: no-parallel-replicas
-- A join whose result must be empty because an input is filtered by an empty `IN (subquery)` set returns without reading its other input.

DROP ROW POLICY IF EXISTS p_left_policy ON t_left_policy;
DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_left_memory;
DROP TABLE IF EXISTS t_left_policy;
DROP TABLE IF EXISTS t_left_merge;
DROP TABLE IF EXISTS t_empty;
DROP TABLE IF EXISTS t_keys;
DROP TABLE IF EXISTS t_set_engine;
DROP TABLE IF EXISTS t_join;
DROP TABLE IF EXISTS t_join_right;
DROP TABLE IF EXISTS t_join_semi;

CREATE TABLE t_left (k String, j String, v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE t_left_memory (k String, j String, v UInt64) ENGINE = Memory;
CREATE TABLE t_left_policy (k String, j String, v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE t_left_merge (k String, j String, v UInt64) ENGINE = Merge(currentDatabase(), '^t_left$');
CREATE TABLE t_empty (k String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_keys (k String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_set_engine (k String) ENGINE = Set;
CREATE TABLE t_join (j String, x UInt64) ENGINE = Join(ANY, LEFT, j);
CREATE TABLE t_join_right (j String) ENGINE = Join(ALL, RIGHT, j);
CREATE TABLE t_join_semi (j String) ENGINE = Join(SEMI, RIGHT, j);

INSERT INTO t_left SELECT toString(number % 10), toString(number % 7), number FROM numbers(100);
INSERT INTO t_left_memory SELECT k, j, v FROM t_left;
INSERT INTO t_left_policy SELECT k, j, v FROM t_left;
INSERT INTO t_keys VALUES ('1'), ('2');
INSERT INTO t_join SELECT toString(number), number FROM numbers(7);
INSERT INTO t_join_right SELECT toString(number) FROM numbers(7);
INSERT INTO t_join_semi SELECT toString(number) FROM numbers(7);
CREATE ROW POLICY p_left_policy ON t_left_policy USING k IN (SELECT k FROM t_empty) TO ALL;

SET query_plan_join_swap_table = 'false', query_plan_filter_push_down = 1;

-- The right side throws if any of its rows is evaluated.

SELECT 'window', count(), sum(s)
FROM (SELECT l.k AS k, sum(l.v) OVER (PARTITION BY l.k) AS s, r.j AS rj FROM t_left AS l
    LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j)
WHERE k IN (SELECT k FROM t_empty);

SELECT 'rn', count()
FROM (SELECT k, row_number() OVER (PARTITION BY k ORDER BY v) AS rn
    FROM (SELECT l.k AS k, l.v AS v FROM t_left AS l
        LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j))
WHERE rn = 1 AND k IN (SELECT k FROM t_empty);

SELECT 'left hash', count() FROM t_left AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS join_algorithm = 'hash';

SELECT 'left parallel_hash', count() FROM t_left AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS join_algorithm = 'parallel_hash';

SELECT 'left grace_hash', count() FROM t_left AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS join_algorithm = 'grace_hash';

SELECT 'left partial_merge', count() FROM t_left AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS join_algorithm = 'partial_merge';

SELECT 'left full_sorting_merge', count() FROM t_left AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS join_algorithm = 'full_sorting_merge';

SELECT 'left auto', count() FROM t_left AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS join_algorithm = 'auto';

SELECT 'left default', count() FROM t_left AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS join_algorithm = 'default';

SELECT 'inner', count() FROM t_left AS l
INNER JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty);

SELECT 'cross', count(), max(r.j) FROM t_left AS l
CROSS JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r
WHERE l.k IN (SELECT k FROM t_empty);

SELECT 'left semi', count() FROM t_left AS l
LEFT SEMI JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty);

SELECT 'left anti', count() FROM t_left AS l
LEFT ANTI JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty);

SELECT 'prewhere', count() FROM t_left AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS optimize_move_to_prewhere = 1;

SELECT 'where', count() FROM t_left AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS optimize_move_to_prewhere = 0;

SELECT 'chain', count() FROM t_left AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j2 FROM numbers(10)) AS r2 ON l.j = r2.j2
WHERE l.k IN (SELECT k FROM t_empty);

SELECT 'window chain', count(), sum(s)
FROM (SELECT l.k AS k, sum(l.v) OVER (PARTITION BY l.k) AS s, r.j AS rj, r2.j2 AS rj2 FROM t_left AS l
    LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
    LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j2 FROM numbers(10)) AS r2 ON l.j = r2.j2)
WHERE k IN (SELECT k FROM t_empty);

SELECT 'null in', count() FROM t_left AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS transform_null_in = 1;

SELECT 'left ie_join', count() FROM t_left AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), 0, number) AS a, number AS b FROM numbers(10)) AS r ON l.v < r.a AND l.v > r.b
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS join_algorithm = 'ie_join';

SELECT 'left full_sorting_merge swapped', count() FROM t_left AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 'true';

SELECT 'inner right filter', count() FROM (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r
INNER JOIN t_left AS l ON r.j = l.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS join_algorithm = 'full_sorting_merge';

SELECT 'left full_sorting_merge onfly', count() FROM t_left AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS join_algorithm = 'full_sorting_merge', max_rows_in_set_to_optimize_join = 1000;

SELECT 'filled chain', count() FROM t_left AS l
ANY LEFT JOIN t_join AS tj ON l.j = tj.j
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty);

SELECT 'memory prewhere', count() FROM t_left_memory AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS optimize_move_to_prewhere = 1;

SELECT 'row policy', count() FROM t_left_policy AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j;

SELECT 'merge table', count() FROM t_left_merge AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty);

SELECT 'right semi', count() FROM t_left AS l
RIGHT SEMI JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r
ON l.j = r.j AND l.k IN (SELECT k FROM t_empty);

SELECT 'left semi swapped', count() FROM (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r
LEFT SEMI JOIN t_left AS l ON r.j = l.j AND l.k IN (SELECT k FROM t_empty) SETTINGS query_plan_join_swap_table = 'true';

SELECT 'left semi ie_join', count() FROM (SELECT if(throwIf(number >= 0, 'right side was read'), 0, number) AS a, number AS b FROM numbers(10)) AS r
LEFT SEMI JOIN t_left AS l ON r.a > l.v AND r.b < l.v AND l.k IN (SELECT k FROM t_empty) SETTINGS join_algorithm = 'ie_join';

SELECT 'filled semi chain', count() FROM t_left AS l
RIGHT SEMI JOIN t_join_semi AS tjs ON l.j = tjs.j
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON tjs.j = r.j
PREWHERE l.k IN (SELECT k FROM t_empty);

SELECT 'runtime filter chain', count() FROM t_left AS l
INNER JOIN t_left_memory AS m ON l.v = m.v
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE m.k IN (SELECT k FROM t_empty)
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, query_plan_optimize_join_order_limit = 1;

SELECT 'distinct', count() FROM (SELECT DISTINCT k, j FROM t_left) AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty);

SELECT 'array join', count() FROM (SELECT k, j, a FROM t_left ARRAY JOIN [1, 2] AS a) AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty);

SELECT 'array join element', count() FROM (SELECT k, j, a FROM t_left ARRAY JOIN [1, 2] AS a) AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE toString(l.a) IN (SELECT k FROM t_empty) SETTINGS query_plan_fuse_filter_into_array_join = 1;

SELECT 'window subquery', count(), sum(l.rn) FROM (SELECT k, j, row_number() OVER (PARTITION BY k ORDER BY v) AS rn FROM t_left) AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty);

SELECT 'limit by', count() FROM (SELECT k, j FROM t_left ORDER BY v LIMIT 2 BY k) AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS query_plan_filter_push_down_below_limit_by = 1;

SELECT 'group by', count() FROM (SELECT k, any(j) AS j FROM t_left GROUP BY k) AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty);

SELECT 'merging aggregated', count() FROM (SELECT k, any(j) AS j FROM remote('127.0.0.{1,1}', currentDatabase(), t_left) GROUP BY k) AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) SETTINGS prefer_localhost_replica = 0;

SELECT 'totals dropped', count() FROM (SELECT k, any(j) AS j FROM (SELECT k, any(j) AS j FROM t_left GROUP BY k WITH TOTALS) GROUP BY k) AS l
LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty);

-- The join still runs in full when its result does not have to be empty.

SELECT 'not in', count() FROM t_left AS l
LEFT JOIN (SELECT toString(number) AS j FROM numbers(5)) AS r ON l.j = r.j
WHERE l.k NOT IN (SELECT k FROM t_empty);

SELECT 'non-empty set', count(), countIf(r.j != '') FROM t_left AS l
LEFT JOIN (SELECT toString(number) AS j FROM numbers(5)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_keys);

SELECT 'right', count() FROM t_left AS l
RIGHT JOIN (SELECT toString(number) AS j FROM numbers(5)) AS r ON l.j = r.j AND l.k IN (SELECT k FROM t_empty);

SELECT 'full', count() FROM t_left AS l
FULL JOIN (SELECT toString(number) AS j FROM numbers(5)) AS r ON l.j = r.j AND l.k IN (SELECT k FROM t_empty);

SELECT 'totals';
SELECT l.k, r.c FROM t_left AS l
LEFT JOIN (SELECT toString(number % 3) AS j, count() AS c FROM numbers(6) GROUP BY j WITH TOTALS) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty);

SELECT 'totals left';
SELECT l.k, l.c, r.j FROM (SELECT k, count() AS c FROM t_left GROUP BY k WITH TOTALS) AS l
LEFT JOIN (SELECT toString(number) AS j FROM numbers(5)) AS r ON l.k = r.j
WHERE l.k IN (SELECT k FROM t_empty);

SELECT 'right then left', count() FROM t_left AS l
RIGHT JOIN (SELECT toString(number) AS j FROM numbers(5)) AS r ON l.j = r.j AND l.k IN (SELECT k FROM t_empty)
LEFT JOIN (SELECT toString(number) AS j2 FROM numbers(5)) AS r2 ON l.j = r2.j2;

SELECT 'left, empty right', count() FROM t_left AS l
LEFT JOIN (SELECT toString(number) AS j FROM numbers(5)) AS r ON l.j = r.j AND r.j IN (SELECT k FROM t_empty);

SELECT 'filled right chain', count() FROM t_left AS l
RIGHT JOIN t_join_right AS tjr ON l.j = tjr.j
LEFT JOIN (SELECT toString(number) AS j2 FROM numbers(5)) AS r2 ON l.j = r2.j2
PREWHERE l.k IN (SELECT k FROM t_empty);

SELECT 'or', count() FROM t_left AS l
LEFT JOIN (SELECT toString(number) AS j FROM numbers(5)) AS r ON l.j = r.j
WHERE l.k IN (SELECT k FROM t_empty) OR l.v >= 50;

SELECT 'right anti', count() FROM t_left AS l
RIGHT ANTI JOIN (SELECT toString(number) AS j FROM numbers(10)) AS r ON l.j = r.j AND l.k IN (SELECT k FROM t_empty);

-- The pipeline gets the short-circuit only for an immutable set.

SELECT 'explain subquery set', count() > 0 FROM (EXPLAIN PIPELINE SELECT count() FROM t_left AS l
    LEFT JOIN (SELECT if(throwIf(number >= 0, 'right side was read'), '', toString(number)) AS j FROM numbers(10)) AS r ON l.j = r.j
    WHERE l.k IN (SELECT k FROM t_empty) SETTINGS join_algorithm = 'hash')
WHERE explain LIKE '%EmptySetShortCircuitTransform%';

SELECT 'explain Set table', count() > 0 FROM (EXPLAIN PIPELINE SELECT count() FROM t_left AS l
    LEFT JOIN (SELECT toString(number) AS j FROM numbers(5)) AS r ON l.j = r.j
    WHERE l.k IN t_set_engine)
WHERE explain LIKE '%EmptySetShortCircuitTransform%';

SELECT 'explain not in', count() > 0 FROM (EXPLAIN PIPELINE SELECT count() FROM t_left AS l
    LEFT JOIN (SELECT toString(number) AS j FROM numbers(5)) AS r ON l.j = r.j
    WHERE l.k NOT IN (SELECT k FROM t_empty))
WHERE explain LIKE '%EmptySetShortCircuitTransform%';

DROP ROW POLICY p_left_policy ON t_left_policy;
DROP TABLE t_left;
DROP TABLE t_left_memory;
DROP TABLE t_left_policy;
DROP TABLE t_left_merge;
DROP TABLE t_empty;
DROP TABLE t_keys;
DROP TABLE t_set_engine;
DROP TABLE t_join;
DROP TABLE t_join_right;
DROP TABLE t_join_semi;
