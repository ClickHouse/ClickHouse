-- Under group_by_overflow_mode = 'any' the merge path hands a null place for every key that did
-- not fit into the frozen hash table; such a row must be skipped, not written to.
--
-- The overflow arms only produce that null place when the merge goes through mergeOnBlock, so
-- they pin optimize_aggregation_in_order = 0: in-order aggregation merges via mergeOnBlockSmall,
-- which never freezes the hash table. The pin must be in the OUTER SETTINGS to take precedence
-- over the test runner's client-level randomization. force_optimize_projection_name keeps each
-- arm on the aggregate-projection merge path, and the exact surviving-row counts below hold only
-- while both hold, so an arm that stops covering the null place fails instead of passing.
-- Each oracle counts NULL as a mismatch, because sum() skips NULL rows and a bare s != k
-- therefore stays 0 when a surviving row comes back NULL instead of its aggregate.
-- Merges stay stopped so the three parts below reach the SELECT separately: one projection
-- part per part is what carries enough keys past the frozen hash table for a null place to
-- appear, and over a single merged part every count below is 3000 instead.

DROP TABLE IF EXISTS t_orfill;

CREATE TABLE t_orfill (k UInt64, k2 UInt64, v UInt64,
    PROJECTION p (SELECT k, sumOrNull(v), sumOrDefault(v), sumTupleOrNull(tuple(v)), sumOrNullTuple(tuple(v)) GROUP BY k),
    PROJECTION p2 (SELECT k, k2, sumOrNull(v) GROUP BY k, k2))
ENGINE = MergeTree ORDER BY tuple();

SYSTEM STOP MERGES t_orfill;

INSERT INTO t_orfill SELECT number, number % 97, number FROM numbers(1000);
INSERT INTO t_orfill SELECT number + 1000, number % 97, number + 1000 FROM numbers(1000);
INSERT INTO t_orfill SELECT number + 2000, number % 97, number + 2000 FROM numbers(1000);

SELECT 'no overflow';
SELECT count(), sum(s IS NULL OR s != k) FROM (SELECT k, sumOrNull(v) AS s FROM t_orfill GROUP BY k)
SETTINGS optimize_use_projections = 1, max_threads = 1;

SELECT 'overflow any OrNull';
SELECT count(), sum(s IS NULL OR s != k) FROM (SELECT k, sumOrNull(v) AS s FROM t_orfill GROUP BY k)
SETTINGS optimize_use_projections = 1, max_threads = 1, optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 10, group_by_overflow_mode = 'any',
    force_optimize_projection_name = 'p';

SELECT 'overflow any OrDefault';
SELECT count(), sum(s IS NULL OR s != k) FROM (SELECT k, sumOrDefault(v) AS s FROM t_orfill GROUP BY k)
SETTINGS optimize_use_projections = 1, max_threads = 1, optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 10, group_by_overflow_mode = 'any',
    force_optimize_projection_name = 'p';

SELECT 'overflow any OrNull(Tuple)';
SELECT count(), sum(s.1 IS NULL OR s.1 != k) FROM (SELECT k, sumTupleOrNull(tuple(v)) AS s FROM t_orfill GROUP BY k)
SETTINGS optimize_use_projections = 1, max_threads = 1, optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 10, group_by_overflow_mode = 'any',
    force_optimize_projection_name = 'p';

SELECT 'overflow any Tuple(OrNull)';
SELECT count(), sum(s.1 IS NULL OR s.1 != k) FROM (SELECT k, sumOrNullTuple(tuple(v)) AS s FROM t_orfill GROUP BY k)
SETTINGS optimize_use_projections = 1, max_threads = 1, optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 10, group_by_overflow_mode = 'any',
    force_optimize_projection_name = 'p';

SELECT 'overflow any two keys';
-- k2 is a stored column, not an expression over k, so optimize_group_by_function_keys cannot
-- drop it and the aggregation really uses the fixed-keys (keys128) method.
SELECT count() > 0 FROM (EXPLAIN SELECT k, k2, sumOrNull(v) FROM t_orfill GROUP BY k, k2)
WHERE explain ILIKE '%Keys: k, k2%';
SELECT count(), sum(s IS NULL OR s != k) FROM (SELECT k, k2, sumOrNull(v) AS s FROM t_orfill GROUP BY k, k2)
SETTINGS optimize_use_projections = 1, max_threads = 1, optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 10, group_by_overflow_mode = 'any',
    force_optimize_projection_name = 'p2';

SELECT 'overflow throw';
SELECT count(), sum(s IS NULL OR s != k) FROM (SELECT k, sumOrNull(v) AS s FROM t_orfill GROUP BY k)
SETTINGS optimize_use_projections = 1, max_threads = 1, optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 10, group_by_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_orfill;
