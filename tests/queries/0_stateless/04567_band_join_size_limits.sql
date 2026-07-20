-- Tags: no-old-analyzer

-- The band join materializes only the interval side, so `max_rows_in_join` /
-- `max_bytes_in_join` apply to it alone: the point side may be arbitrarily large. With the
-- default `join_overflow_mode = 'throw'` an exceeded limit fails the query; with 'break' the
-- operator keeps what fits and drops the rest of the interval input.

-- Keep the written join order: the band join detects only the point-side-on-the-left
-- orientation for now, so a planner swap would silently change the executed algorithm.
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

SET join_overflow_mode = 'break';
SELECT 'break', count() >= 0 FROM lim_p p JOIN lim_i i ON p.t >= i.lo AND p.t <= i.hi;

SET max_rows_in_join = 0;
SET join_overflow_mode = 'throw';
SET max_bytes_in_join = 100;
SELECT count() FROM lim_p p JOIN lim_i i ON p.t >= i.lo AND p.t <= i.hi; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

DROP TABLE lim_p;
DROP TABLE lim_i;
