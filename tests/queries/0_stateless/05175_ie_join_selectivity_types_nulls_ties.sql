-- Tags: no-old-analyzer

-- The statistics-based choice of the IEJoin key conditions with Nullable key columns, key
-- columns of different numeric types, tied estimates, and conditions without an estimate.

SET join_algorithm = 'ie_join';
SET join_use_nulls = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
-- The printed conditions are mirrored when the join order optimizer swaps the sides
-- (e.g. under randomized `query_plan_optimize_join_order_randomize`); disable it to keep them stable.
SET query_plan_optimize_join_order_limit = 0;

DROP TABLE IF EXISTS t_sel_null_l;
DROP TABLE IF EXISTS t_sel_null_r;
DROP TABLE IF EXISTS t_sel_mixed_l;
DROP TABLE IF EXISTS t_sel_mixed_r;
DROP TABLE IF EXISTS t_sel_ties;
DROP TABLE IF EXISTS t_sel_axis_l;
DROP TABLE IF EXISTS t_sel_axis_r;

SELECT '-- a mostly-NULL key column: NULL rows fail the inequality, so the condition is the most selective';
CREATE TABLE t_sel_null_l (a1 UInt32, a2 UInt32, a3 Nullable(UInt32))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_sel_null_r (b1 UInt32, b2 UInt32, b3 UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

-- Non-NULL `a3` is in [0, 90]: sel(a1 < b1) ~ 0.5, sel(a2 < b2) ~ 0.875, sel(a3 < b3) ~ 0.95, which would
-- pick (a1 < b1, a2 < b2); 99% of `a3` is NULL, which scales sel(a3 < b3) down to ~ 0.01, the best pick.
INSERT INTO t_sel_null_l SELECT number % 1000, number % 1000, if(number % 100 = 0, intDiv(number, 10) % 100, NULL) FROM numbers(1000);
INSERT INTO t_sel_null_r SELECT number % 1000, 500 + number % 1000, number % 1000 FROM numbers(1000);

SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_null_l AS l JOIN t_sel_null_r AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT count(), sum(a1 + a2 + a3 + b1 + b2 + b3) FROM t_sel_null_l AS l JOIN t_sel_null_r AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3;

-- The oracle: the same predicate as a filter over CROSS JOIN.
SELECT count(), sum(a1 + a2 + a3 + b1 + b2 + b3) FROM t_sel_null_l AS l, t_sel_null_r AS r
WHERE l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS join_algorithm = 'hash';

SELECT '-- different numeric types share one axis: UInt32 keys against Int64 keys with negative values';
CREATE TABLE t_sel_mixed_l (a1 UInt32, a2 UInt32, a3 UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_sel_mixed_r (b1 Int64, b2 Int64, b3 Int64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

-- sel(a1 < b1) ~ 0.5, sel(a2 < b2) = 1, sel(a3 < b3) ~ 0.00005: `b3` is in [-990, 9].
INSERT INTO t_sel_mixed_l SELECT number % 1000, number % 1000, number % 1000 FROM numbers(1000);
INSERT INTO t_sel_mixed_r SELECT number % 1000, 1000 + number % 1000, toInt64(number % 1000) - 990 FROM numbers(1000);

SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_mixed_l AS l JOIN t_sel_mixed_r AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT count(), sum(a1 + a2 + a3 + b1 + b2 + b3) FROM t_sel_mixed_l AS l JOIN t_sel_mixed_r AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3;

SELECT count(), sum(a1 + a2 + a3 + b1 + b2 + b3) FROM t_sel_mixed_l AS l, t_sel_mixed_r AS r
WHERE l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS join_algorithm = 'hash';

SELECT '-- identical estimates: the first two in syntax order';
CREATE TABLE t_sel_ties (x UInt32, y UInt32, z UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

INSERT INTO t_sel_ties SELECT number % 1000, number % 1000, number % 1000 FROM numbers(1000);

SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_ties AS l JOIN t_sel_ties AS r
    ON l.z < r.x AND l.y < r.y AND l.x < r.z
) WHERE explain LIKE '%Conditions:%';

SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_ties AS l JOIN t_sel_ties AS r
    ON l.x < r.z AND l.z < r.x AND l.y < r.y
) WHERE explain LIKE '%Conditions:%';

SELECT count() FROM t_sel_ties AS l JOIN t_sel_ties AS r
ON l.z < r.x AND l.y < r.y AND l.x < r.z;

SELECT count() FROM t_sel_ties AS l, t_sel_ties AS r
WHERE l.z < r.x AND l.y < r.y AND l.x < r.z
SETTINGS join_algorithm = 'hash';

SELECT '-- a condition without an estimate disables the choice: Date against Date32';
CREATE TABLE t_sel_axis_l (a1 UInt32, a2 UInt32, d Date)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_sel_axis_r (b1 UInt32, b2 UInt32, d32 Date32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

INSERT INTO t_sel_axis_l SELECT number % 1000, number % 1000, toDate('2024-01-01') + number % 100 FROM numbers(1000);
INSERT INTO t_sel_axis_r SELECT number % 1000, 1000 + number % 1000, toDate32('2024-01-01') + number % 1000 FROM numbers(1000);

-- With estimates the always-true `a2 < b2` would never be chosen; without one for `d < d32`
-- (the two sides are not numbers, so they share no axis) the first two in syntax order are used.
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_axis_l AS l JOIN t_sel_axis_r AS r
    ON l.a2 < r.b2 AND l.d < r.d32 AND l.a1 < r.b1
) WHERE explain LIKE '%Conditions:%';

SELECT count() FROM t_sel_axis_l AS l JOIN t_sel_axis_r AS r
ON l.a2 < r.b2 AND l.d < r.d32 AND l.a1 < r.b1;

SELECT count() FROM t_sel_axis_l AS l, t_sel_axis_r AS r
WHERE l.a2 < r.b2 AND l.d < r.d32 AND l.a1 < r.b1
SETTINGS join_algorithm = 'hash';

SELECT '-- a key behind an expression has no statistics: syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_sel_mixed_l AS l JOIN t_sel_mixed_r AS r
    ON l.a2 < r.b2 AND negate(l.a1) < r.b1 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT count() FROM t_sel_mixed_l AS l JOIN t_sel_mixed_r AS r
ON l.a2 < r.b2 AND negate(l.a1) < r.b1 AND l.a3 < r.b3;

SELECT count() FROM t_sel_mixed_l AS l, t_sel_mixed_r AS r
WHERE l.a2 < r.b2 AND negate(l.a1) < r.b1 AND l.a3 < r.b3
SETTINGS join_algorithm = 'hash';

DROP TABLE t_sel_null_l;
DROP TABLE t_sel_null_r;
DROP TABLE t_sel_mixed_l;
DROP TABLE t_sel_mixed_r;
DROP TABLE t_sel_ties;
DROP TABLE t_sel_axis_l;
DROP TABLE t_sel_axis_r;
