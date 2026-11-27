-- Tags: no-old-analyzer

-- Two conditions comparing the same column to two unrelated columns in opposite directions
-- (`r.x < l.a AND l.a < r.y`) look like a band, but `x` and `y` are not the ends of an interval,
-- so the pair is not selective. Their marginals sum to exactly 1, and the band estimate
-- P(A) + P(B) - 1 collapses to 0; that must not make the pair win over a genuinely selective one.
-- https://github.com/ClickHouse/ClickHouse/issues/120092

SET join_algorithm = 'ie_join';
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET query_plan_optimize_join_order_limit = 0;
SET query_plan_join_swap_table = 0;

DROP TABLE IF EXISTS t_pb_l;
DROP TABLE IF EXISTS t_pb_r;

CREATE TABLE t_pb_l (a UInt32, c UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_pb_r (x UInt32, y UInt32, d UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

INSERT INTO t_pb_l SELECT number % 1000, 500 + number % 500 FROM numbers(4000);
INSERT INTO t_pb_r SELECT number % 1000, (number * 7) % 1000, number % 515 FROM numbers(4000);

SELECT '-- row pairs passing each candidate key pair';
SELECT 'c < d AND a > x', count() FROM t_pb_l AS l JOIN t_pb_r AS r ON l.c < r.d AND l.a > r.x;
SELECT 'a > x AND a < y', count() FROM t_pb_l AS l JOIN t_pb_r AS r ON l.a > r.x AND l.a < r.y;

SELECT '-- chosen key conditions';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_pb_l AS l JOIN t_pb_r AS r
    ON l.c < r.d AND r.x < l.a AND l.a < r.y
) WHERE explain LIKE '%Conditions:%';

SELECT '-- result does not depend on the choice';
SELECT count() FROM t_pb_l AS l JOIN t_pb_r AS r ON l.c < r.d AND r.x < l.a AND l.a < r.y;
SELECT count() FROM t_pb_l AS l, t_pb_r AS r WHERE l.c < r.d AND r.x < l.a AND l.a < r.y
SETTINGS join_algorithm = 'hash';

DROP TABLE t_pb_l;
DROP TABLE t_pb_r;

-- Here the marginals sum to slightly more than 1 (`y` reaches above `a`), so the band estimate
-- is small but positive; only the order of the ends refutes the band: min(x) = 10 > min(y) = 0,
-- impossible for `x <= y` on every row.
CREATE TABLE t_pb_l (a UInt32, c UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_pb_r (x UInt32, y UInt32, d UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

INSERT INTO t_pb_l SELECT number % 1000, number % 1000 FROM numbers(4000);
INSERT INTO t_pb_r SELECT 10 + number % 980, (number * 7) % 1010, number % 40 FROM numbers(4000);

SELECT '-- ends out of order: row pairs passing each candidate key pair';
SELECT 'c < d AND a > x', count() FROM t_pb_l AS l JOIN t_pb_r AS r ON l.c < r.d AND l.a > r.x;
SELECT 'a > x AND a < y', count() FROM t_pb_l AS l JOIN t_pb_r AS r ON l.a > r.x AND l.a < r.y;

SELECT '-- chosen key conditions';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_pb_l AS l JOIN t_pb_r AS r
    ON l.c < r.d AND r.x < l.a AND l.a < r.y
) WHERE explain LIKE '%Conditions:%';

SELECT '-- result does not depend on the choice';
SELECT count() FROM t_pb_l AS l JOIN t_pb_r AS r ON l.c < r.d AND r.x < l.a AND l.a < r.y;
SELECT count() FROM t_pb_l AS l, t_pb_r AS r WHERE l.c < r.d AND r.x < l.a AND l.a < r.y
SETTINGS join_algorithm = 'hash';

DROP TABLE t_pb_l;
DROP TABLE t_pb_r;
