-- Under group_by_overflow_mode = 'any' the merge path hands a null place for every key that
-- did not fit into the frozen hash table; such a row must be skipped, not written to.

DROP TABLE IF EXISTS t_orfill;

CREATE TABLE t_orfill (k UInt64, v UInt64,
    PROJECTION p (SELECT k, sumOrNull(v), sumOrDefault(v), sumTupleOrNull(tuple(v)) GROUP BY k))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_orfill SELECT number, number FROM numbers(1000);
INSERT INTO t_orfill SELECT number + 1000, number + 1000 FROM numbers(1000);
INSERT INTO t_orfill SELECT number + 2000, number + 2000 FROM numbers(1000);

SELECT 'no overflow';
SELECT count(), sum(s != k) FROM (SELECT k, sumOrNull(v) AS s FROM t_orfill GROUP BY k)
SETTINGS optimize_use_projections = 1, max_threads = 1;

SELECT 'overflow any OrNull';
SELECT count() > 0, sum(s != k) FROM (SELECT k, sumOrNull(v) AS s FROM t_orfill GROUP BY k)
SETTINGS optimize_use_projections = 1, max_threads = 1,
    max_rows_to_group_by = 10, group_by_overflow_mode = 'any';

SELECT 'overflow any OrDefault';
SELECT count() > 0, sum(s != k) FROM (SELECT k, sumOrDefault(v) AS s FROM t_orfill GROUP BY k)
SETTINGS optimize_use_projections = 1, max_threads = 1,
    max_rows_to_group_by = 10, group_by_overflow_mode = 'any';

SELECT 'overflow any Tuple(OrNull)';
SELECT count() > 0, sum(s.1 != k) FROM (SELECT k, sumTupleOrNull(tuple(v)) AS s FROM t_orfill GROUP BY k)
SETTINGS optimize_use_projections = 1, max_threads = 1,
    max_rows_to_group_by = 10, group_by_overflow_mode = 'any';

SELECT 'overflow any two keys';
SELECT count() > 0, sum(s != k) FROM
    (SELECT k, k + 7 AS k2, sumOrNull(v) AS s FROM t_orfill GROUP BY k, k2)
SETTINGS optimize_use_projections = 1, max_threads = 1,
    max_rows_to_group_by = 10, group_by_overflow_mode = 'any';

SELECT 'overflow throw';
-- With in-order aggregation the limit is not exceeded, so nothing is thrown.
SELECT count(), sum(s != k) FROM (SELECT k, sumOrNull(v) AS s FROM t_orfill GROUP BY k)
SETTINGS optimize_use_projections = 1, max_threads = 1, optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 10, group_by_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_orfill;
