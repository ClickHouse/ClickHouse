-- Regression test for the monotonicity factor of date functions over a `DateTime64` key with
-- sub-second precision and pre-epoch values.
--
-- For a negative `DateTime64` with a fractional part, `TransformDateTime64::executeExtendedResult`
-- must round the value towards negative infinity (like `execute` does). Otherwise a key in the
-- last fractional second of a year (e.g. 1964-12-31 23:59:59.9) is attributed to the next year by
-- the monotonicity factor while the actual function execution attributes it to the previous one:
-- the granule [1964-12-31 23:59:59.9, 1965-08-15] would be wrongly declared monotonic for
-- `toMonth`, and `WHERE toMonth(d) = 12` would prune the granule that contains the matching row.
-- The countIf line is the ground truth (full scan, no key pruning) and must match.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_subsec;
CREATE TABLE t_subsec (d DateTime64(1, 'UTC')) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_subsec VALUES ('1964-12-31 23:59:59.9'),('1965-01-15 00:00:00.0'),('1965-02-15 00:00:00.0'),('1965-03-15 00:00:00.0'),('1965-04-15 00:00:00.0'),('1965-05-15 00:00:00.0'),('1965-06-15 00:00:00.0'),('1965-07-15 00:00:00.0'),('1965-08-15 00:00:00.0');

SELECT count() FROM t_subsec WHERE toMonth(d) = 12;
SELECT countIf(toMonth(d) = 12) FROM t_subsec;

DROP TABLE t_subsec;

-- The same rounding bug was user-visible in the extended-results execution path: the value
-- belongs to December 1969 (`toMonth` = 12, `toDate32` = 1969-12-31), but the extended
-- `toStartOfMonth` attributed it to January 1970.
SELECT toStartOfMonth(toDateTime64('1969-12-31 23:59:59.5', 1)) SETTINGS enable_extended_results_for_datetime_functions = 1;
