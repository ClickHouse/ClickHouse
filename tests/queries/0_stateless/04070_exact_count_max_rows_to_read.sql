-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100129
-- SELECT count() with max_rows_to_read / force_primary_key should not throw
-- TOO_MANY_ROWS when data is split across multiple parts with non-aligned
-- granule boundaries, because the exact count optimization computes the result
-- from the primary key without reading data.

DROP TABLE IF EXISTS t_exact_count_force_pk;

CREATE TABLE t_exact_count_force_pk
(
    key UInt64,
    value String
) ENGINE = MergeTree() ORDER BY key
SETTINGS index_granularity = 100;

SYSTEM STOP MERGES t_exact_count_force_pk;

-- Insert multiple parts with different sizes to create non-aligned granule boundaries.
INSERT INTO t_exact_count_force_pk SELECT number, toString(number) FROM numbers(150);
INSERT INTO t_exact_count_force_pk SELECT number + 1000, toString(number) FROM numbers(200);
INSERT INTO t_exact_count_force_pk SELECT number + 3000, toString(number) FROM numbers(250);

-- Enable projection optimization so the exact count path is available.
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;

-- force_primary_key implies max_rows_to_read checks during PK filtering.
-- Set a limit that is smaller than the total rows but larger than any single part.
SET max_rows_to_read = 300;
SET read_overflow_mode = 'throw';
SET force_primary_key = 1;

-- count() should succeed via exact count optimization, not throw TOO_MANY_ROWS.
SELECT count() FROM t_exact_count_force_pk;

-- A range that covers all parts should also work for count().
SELECT count() FROM t_exact_count_force_pk WHERE key < 5000;

-- Non-count queries should still respect the limit and throw.
-- Use a WHERE on the primary key to satisfy force_primary_key, but cover all rows.
SELECT * FROM t_exact_count_force_pk WHERE key < 5000 FORMAT Null; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_exact_count_force_pk;
