-- Tags: no-old-analyzer

-- The band join materializes only the interval side, so `max_rows_in_join` /
-- `max_bytes_in_join` apply to it alone: the point side may be arbitrarily large. With the
-- default `join_overflow_mode = 'throw'` an exceeded limit fails the query; with 'break' the
-- operator keeps what fits and drops the rest of the interval input.

-- Keep the written join order so the checks below exercise the orientation as written
-- instead of whatever the join order optimizer prefers.
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';

DROP TABLE IF EXISTS lim_p;
DROP TABLE IF EXISTS lim_i;

CREATE TABLE lim_p (id Int32, t Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE lim_i (id Int32, lo Int64, hi Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO lim_p SELECT number, number % 50 FROM numbers(1000);
INSERT INTO lim_i SELECT number, number % 40, number % 40 + 5 FROM numbers(100);

SELECT 'plan', count() > 0 FROM (EXPLAIN SELECT count() FROM lim_p p JOIN lim_i i ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%BandJoin%';

-- A limit above the interval side does not fire even though the point side exceeds it
SET max_rows_in_join = 500;
SELECT 'under limit', count() FROM lim_p p JOIN lim_i i ON p.t >= i.lo AND p.t <= i.hi;

SET max_rows_in_join = 50;
SELECT count() FROM lim_p p JOIN lim_i i ON p.t >= i.lo AND p.t <= i.hi; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- 'break' keeps the accumulated interval prefix including the chunk that reaches the limit
-- and drops the rest: the result must be a non-empty subset of the unlimited join and every
-- kept pair must still satisfy the band.
SET join_overflow_mode = 'break';
SELECT 'break',
    count() > 0 AS nonempty,
    countIf(NOT (t >= lo AND t <= hi)) = 0 AS all_valid,
    count() <= (SELECT count() FROM lim_p p JOIN lim_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS max_rows_in_join = 0) AS subset
FROM (SELECT p.t AS t, i.lo AS lo, i.hi AS hi FROM lim_p p JOIN lim_i i ON p.t >= i.lo AND p.t <= i.hi);

-- The same with the interval stream arriving in many blocks: a strict prefix of the input
-- survives; every point key is below the kept `lo` prefix's ceiling, so no match is lost.
DROP TABLE IF EXISTS lim_i2;
CREATE TABLE lim_i2 (id Int32, lo Int64, hi Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO lim_i2 SELECT number, number, number + 5 FROM numbers(10000);
SET max_rows_in_join = 5000;
SELECT 'break multi',
    count() > 0 AS nonempty,
    countIf(NOT (t >= lo AND t <= hi)) = 0 AS all_valid,
    count() = (SELECT count() FROM lim_p p JOIN lim_i2 i ON p.t >= i.lo AND p.t <= i.hi SETTINGS max_rows_in_join = 0) AS prefix_covers_all_points
FROM (SELECT p.t AS t, i.lo AS lo, i.hi AS hi FROM lim_p p JOIN lim_i2 i ON p.t >= i.lo AND p.t <= i.hi)
SETTINGS max_block_size = 100;
DROP TABLE lim_i2;

SET max_rows_in_join = 0;
SET join_overflow_mode = 'throw';
SET max_bytes_in_join = 100;
SELECT count() FROM lim_p p JOIN lim_i i ON p.t >= i.lo AND p.t <= i.hi; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

DROP TABLE lim_p;
DROP TABLE lim_i;
