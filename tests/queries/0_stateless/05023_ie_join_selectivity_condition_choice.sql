-- Tags: no-old-analyzer

-- With more than two eligible inequality conditions, the two IEJoin key conditions are chosen
-- by their estimated selectivity from the column min/max statistics instead of the first two
-- in syntax order. The conditions not chosen become a filter / residual condition, so any
-- choice returns the same result.

SET join_algorithm = 'ie_join';
SET join_use_nulls = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
-- The printed conditions are mirrored when the join order optimizer swaps the sides
-- (e.g. under randomized `query_plan_optimize_join_order_randomize`); disable it to keep them stable.
SET query_plan_optimize_join_order_limit = 0;

DROP TABLE IF EXISTS t_sel_l;
DROP TABLE IF EXISTS t_sel_r;

CREATE TABLE t_sel_l (a1 UInt32, a2 UInt32, a3 UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_sel_r (b1 UInt32, b2 UInt32, b3 UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

-- sel(a1 < b1) ~ 0.5, sel(a2 < b2) = 1, sel(a3 < b3) ~ 0.005:
-- the best key pair is (a1 < b1, a3 < b3), the first two in syntax order are (a1 < b1, a2 < b2).
INSERT INTO t_sel_l SELECT number % 1000, number % 1000, (number * 97) % 100000 FROM numbers(1000);
INSERT INTO t_sel_r SELECT number % 1000, 1000 + number % 1000, number % 1000 FROM numbers(1000);

SELECT '-- keys chosen by selectivity';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_l AS l JOIN t_sel_r AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT '-- no statistics: the first two in syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_l AS l JOIN t_sel_r AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
    SETTINGS use_statistics = 0
) WHERE explain LIKE '%Conditions:%';

SELECT '-- results are independent of the choice';
SELECT count(), sum(a1 + a2 + a3 + b1 + b2 + b3) FROM t_sel_l AS l JOIN t_sel_r AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3;

SELECT count(), sum(a1 + a2 + a3 + b1 + b2 + b3) FROM t_sel_l AS l JOIN t_sel_r AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS use_statistics = 0;

-- The oracle: the same predicate as a filter over CROSS JOIN.
SELECT count(), sum(a1 + a2 + a3 + b1 + b2 + b3) FROM t_sel_l AS l, t_sel_r AS r
WHERE l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS join_algorithm = 'hash';

SELECT '-- LEFT JOIN: unchosen conditions become the in-operator residual';
SELECT count(), sum(a1 + a3), countIf(b1 = 0 AND b2 = 0 AND b3 = 0) FROM t_sel_l AS l LEFT JOIN t_sel_r AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3;

SELECT count(), sum(a1 + a3), countIf(b1 = 0 AND b2 = 0 AND b3 = 0) FROM t_sel_l AS l LEFT JOIN t_sel_r AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS use_statistics = 0;

SELECT '-- band pair beats the marginal-greedy pick';
-- Marginals: sel(a1 > lo) ~ 0.5, sel(a1 < hi) ~ 0.5, sel(a3 < b3) ~ 0.05; but the band
-- (lo < a1 AND a1 < hi) is correlated with joint selectivity ~ 0.002, so it is the best pair.
DROP TABLE IF EXISTS t_sel_band;
CREATE TABLE t_sel_band (lo UInt32, hi UInt32, b3 UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

INSERT INTO t_sel_band SELECT number % 1000, number % 1000 + 2, (number * 9973) % 10000 FROM numbers(1000);

SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_l AS l JOIN t_sel_band AS r
    ON r.lo < l.a1 AND l.a1 < r.hi AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT count(), sum(a1 + a3 + lo + hi + b3) FROM t_sel_l AS l JOIN t_sel_band AS r
ON r.lo < l.a1 AND l.a1 < r.hi AND l.a3 < r.b3;

SELECT count(), sum(a1 + a3 + lo + hi + b3) FROM t_sel_l AS l, t_sel_band AS r
WHERE r.lo < l.a1 AND l.a1 < r.hi AND l.a3 < r.b3
SETTINGS join_algorithm = 'hash';

DROP TABLE t_sel_l;
DROP TABLE t_sel_r;
DROP TABLE t_sel_band;
