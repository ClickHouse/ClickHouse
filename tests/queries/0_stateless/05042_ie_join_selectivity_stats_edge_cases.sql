-- Edge cases of the statistics-based choice of the IEJoin key conditions: comparisons of
-- constant (single-point range) columns, and value ranges surviving a LIMIT.

SET join_algorithm = 'ie_join';
SET join_use_nulls = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
-- The printed conditions are mirrored when the join order optimizer swaps the sides
-- (e.g. under randomized `query_plan_optimize_join_order_randomize`); disable it to keep them stable.
SET query_plan_optimize_join_order_limit = 0;
-- Keep the ORDER BY/LIMIT subqueries as explicit plan steps; the top-K read optimization has its
-- own cases at the end.
SET use_top_k_dynamic_filtering = 0;
SET use_skip_indexes_for_top_k = 0;

DROP TABLE IF EXISTS t_sel_edge_l;
DROP TABLE IF EXISTS t_sel_edge_r;

CREATE TABLE t_sel_edge_l (c UInt32, a1 UInt32, a2 UInt32, a3 UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_sel_edge_r (c UInt32, b1 UInt32, b2 UInt32, b3 UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

INSERT INTO t_sel_edge_l SELECT 0, number % 1000, number % 1000, (number * 97) % 100000 FROM numbers(1000);
INSERT INTO t_sel_edge_r SELECT 0, number % 1000, 1000 + number % 1000, number % 1000 FROM numbers(1000);

-- `c` is constant 0 on both sides, so `l.c >= r.c` always holds and must be estimated as 1;
-- a strictness-blind uniform model estimates it as 0, making it the guaranteed (worst) pick.
SELECT '-- always-true condition on equal constant columns is not chosen as a key';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_edge_l AS l JOIN t_sel_edge_r AS r
    ON l.a1 < r.b1 AND l.c >= r.c AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_sel_edge_l AS l JOIN t_sel_edge_r AS r
ON l.a1 < r.b1 AND l.c >= r.c AND l.a3 < r.b3;

-- The oracle: the same predicate as a filter over CROSS JOIN.
SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_sel_edge_l AS l, t_sel_edge_r AS r
WHERE l.a1 < r.b1 AND l.c >= r.c AND l.a3 < r.b3
SETTINGS join_algorithm = 'hash';

-- A limit keeps a subset with an unknown value range (a TopN keeps one end of the sorted
-- range), so the table's min/max no longer describe the join input and the choice falls
-- back to the first two conditions in syntax order.
SELECT '-- TopN drops value ranges: syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_edge_l AS l JOIN (SELECT * FROM t_sel_edge_r ORDER BY b3 LIMIT 900) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_sel_edge_l AS l JOIN (SELECT * FROM t_sel_edge_r ORDER BY b3 LIMIT 900) AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3;

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_sel_edge_l AS l, (SELECT * FROM t_sel_edge_r ORDER BY b3 LIMIT 900) AS r
WHERE l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS join_algorithm = 'hash';

SELECT '-- plain LIMIT also drops value ranges';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_edge_l AS l JOIN (SELECT * FROM t_sel_edge_r LIMIT 900) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

-- Exact row tracking proves that an ORDER BY ... LIMIT above the table size does not truncate.
-- The sort keeps the read exact, so the value ranges remain valid for selecting the most selective conditions.
SELECT '-- ORDER BY and limit above the table size preserve value ranges';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_edge_l AS l JOIN (SELECT * FROM t_sel_edge_r ORDER BY b3 LIMIT 1000000) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

-- Any predicate-filtered input fails closed even when the predicate happens to retain every row:
-- whole-part ranges do not describe the produced relation.
SELECT '-- WHERE on a join input drops value ranges: syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_edge_l AS l JOIN (SELECT * FROM t_sel_edge_r WHERE b2 < 5000) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_sel_edge_l AS l
JOIN (SELECT * FROM t_sel_edge_r WHERE b2 < 5000) AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3;

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_sel_edge_l AS l,
    (SELECT * FROM t_sel_edge_r WHERE b2 < 5000) AS r
WHERE l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS join_algorithm = 'hash';

-- Pin both PREWHERE optimizer modes because the stateless-test randomizer changes this setting.
SELECT '-- the same filter as PREWHERE with optimization enabled';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_edge_l AS l
    JOIN (SELECT * FROM t_sel_edge_r WHERE b2 < 5000 SETTINGS optimize_move_to_prewhere = 1) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT '-- the same filter as PREWHERE with optimization disabled';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_edge_l AS l
    JOIN (SELECT * FROM t_sel_edge_r WHERE b2 < 5000 SETTINGS optimize_move_to_prewhere = 0) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

-- Without ORDER BY, a trivial LIMIT is pushed into the read. Even when it equals the table size,
-- the read cannot prove non-truncation and its whole-part ranges do not describe the produced relation.
SELECT '-- trivial LIMIT equal to the table size still drops value ranges';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_edge_l AS l JOIN (SELECT * FROM t_sel_edge_r LIMIT 1000) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

-- The top-K optimization reads an `ORDER BY ... LIMIT n` input through a running boundary and, with a
-- `minmax` index on the sort column, narrows the read to the granules that can hold the first `n` rows.
-- Neither drops rows from an input of at most `n` rows, so value ranges survive as they do without it.
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;

SELECT '-- top-K read with a limit equal to the table size preserves value ranges';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT * FROM t_sel_edge_r ORDER BY b3 LIMIT 1000
) WHERE explain LIKE '%__topKFilter%';

SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_edge_l AS l JOIN (SELECT * FROM t_sel_edge_r ORDER BY b3 LIMIT 1000) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_sel_edge_l AS l JOIN (SELECT * FROM t_sel_edge_r ORDER BY b3 LIMIT 1000) AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3;

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_sel_edge_l AS l, (SELECT * FROM t_sel_edge_r ORDER BY b3 LIMIT 1000) AS r
WHERE l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS join_algorithm = 'hash';

SELECT '-- top-K read with a limit below the table size drops value ranges';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_edge_l AS l JOIN (SELECT * FROM t_sel_edge_r ORDER BY b3 LIMIT 999) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

-- The same rows as `t_sel_edge_r` in granules of one row ordered by `b3`, with a `minmax` index on it.
-- The granule selection keeps the `n` granules with the best values, so with fewer granules than `n`
-- it keeps them all; one-row granules let `LIMIT 500` drop some.
DROP TABLE IF EXISTS t_sel_edge_topk;
CREATE TABLE t_sel_edge_topk (c UInt32, b1 UInt32, b2 UInt32, b3 UInt32, INDEX idx_b3 b3 TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', index_granularity = 1, index_granularity_bytes = 10485760;

INSERT INTO t_sel_edge_topk SELECT 0, number % 1000, 1000 + number % 1000, number % 1000 FROM numbers(1000);

SELECT '-- top-K granule selection keeping every granule preserves value ranges';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    SELECT * FROM t_sel_edge_topk ORDER BY b3 LIMIT 1000
) WHERE explain LIKE '%Filter TopK Granules%';

SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_edge_l AS l JOIN (SELECT * FROM t_sel_edge_topk ORDER BY b3 LIMIT 1000) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

-- For `LIMIT 500` the granule selection keeps 500 of the 1000 granules, so the table's value ranges no
-- longer describe the rows read.
SELECT '-- top-K granule selection dropping granules drops value ranges';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    SELECT * FROM t_sel_edge_topk ORDER BY b3 LIMIT 500
) WHERE explain LIKE '%Granules: 500/1000%';

SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_edge_l AS l JOIN (SELECT * FROM t_sel_edge_topk ORDER BY b3 LIMIT 500) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_sel_edge_l AS l JOIN (SELECT * FROM t_sel_edge_topk ORDER BY b3 LIMIT 500) AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3;

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_sel_edge_l AS l, (SELECT * FROM t_sel_edge_topk ORDER BY b3 LIMIT 500) AS r
WHERE l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS join_algorithm = 'hash';

DROP TABLE t_sel_edge_l;
DROP TABLE t_sel_edge_r;
DROP TABLE t_sel_edge_topk;
