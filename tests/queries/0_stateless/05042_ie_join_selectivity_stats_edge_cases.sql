-- Tags: no-old-analyzer

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

-- The row estimate cannot prove a limit does not truncate (e.g. a TopN read is already
-- scaled down by its `__topKFilter` prewhere), so even a limit above the table size drops
-- the value ranges.
SELECT '-- limit above the table size also drops value ranges';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_edge_l AS l JOIN (SELECT * FROM t_sel_edge_r ORDER BY b3 LIMIT 1000000) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

DROP TABLE t_sel_edge_l;
DROP TABLE t_sel_edge_r;
