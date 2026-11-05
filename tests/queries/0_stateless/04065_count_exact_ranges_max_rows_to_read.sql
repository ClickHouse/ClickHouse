-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100129
-- SELECT count() with max_rows_to_read / force_primary_key should succeed via
-- exact count optimization when data is split across multiple parts with
-- non-aligned granule boundaries.

DROP TABLE IF EXISTS t_count_exact;

CREATE TABLE t_count_exact (key UInt64, value String)
ENGINE = MergeTree() ORDER BY key
SETTINGS index_granularity = 128;

-- Disable row limits while populating data
SET max_rows_to_read = 0;

-- Create multiple parts
INSERT INTO t_count_exact SELECT number, toString(number) FROM numbers(5000);
INSERT INTO t_count_exact SELECT number + 5000, toString(number) FROM numbers(5000);
INSERT INTO t_count_exact SELECT number + 10000, toString(number) FROM numbers(5000);

SYSTEM STOP MERGES t_count_exact;

-- Enable the settings required for exact count optimization
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;

-- The exact count optimization computes the result from primary key analysis
-- without reading data, so count() queries should bypass max_rows_to_read.
-- The limit must be larger than the boundary-granule rows the optimizer still
-- needs to read (a few granules of 128 rows each ≈ a few hundred rows) but
-- much smaller than the total table size (15 000 rows) so that unoptimized
-- full-table reads still hit the limit.
SET max_rows_to_read = 1000;
SET read_overflow_mode = 'throw';

SELECT count() FROM t_count_exact;
SELECT count() FROM t_count_exact WHERE key < 10000;
SELECT count() FROM t_count_exact WHERE key BETWEEN 2000 AND 8000;

-- Verify that non-count queries still respect the limit
SELECT * FROM t_count_exact FORMAT Null; -- { serverError TOO_MANY_ROWS }

-- Verify with force_primary_key
SET force_primary_key = 1;
SELECT count() FROM t_count_exact WHERE key < 10000;

DROP TABLE t_count_exact;
