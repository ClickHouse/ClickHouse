-- Regression test for STID 0937-4a61:
-- UBSan signed integer overflow reports in `doInsertResultInto` of the `timeSeries*ToGrid`
-- aggregate function family.
--
-- Two distinct overflow patterns were caught on PR #103223 after the `bucketCount` and
-- `bucketIndexForTimestamp` fixes landed:
--
--   1. Loop-carried `current_timestamp += Base::step` accumulator (in the `ToGridSparse`
--      and `InstantValue` implementations) performed one final, unused `+=` on the
--      iteration past the last bucket, signed-overflowing `TimestampType`
--      (`Decimal<Int64>::operator+=`) when `start_timestamp` was near `INT64_MIN` and
--      `step` was near `INT64_MAX`.
--   2. Per-iteration `Base::start_timestamp + i * Base::step` (in `Changes`,
--      `ExtrapolatedValue` and `LinearRegression`) signed-overflowed the same way once
--      `i >= 2`.
--
-- The fix adds `Base::timestampAtIndex` which computes `start + i * step` in unsigned
-- 64-bit arithmetic and reinterprets the result as `TimestampType`, avoiding UB on the
-- boundary values.
--
-- This test covers one public function per internal implementation so that all five
-- `doInsertResultInto` variants are exercised under sanitizer builds. Each query uses
-- the adversarial grid `[INT64_MIN, -1, INT64_MAX - 1]` (`bucket_count = 3`) that
-- triggers both overflow patterns in the pre-fix code.

SET allow_experimental_ts_to_grid_aggregate_function = 1;
SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts_insert_overflow;
CREATE TABLE ts_insert_overflow (timestamp DateTime64(0) NOT NULL, value Float64 NOT NULL)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ts_insert_overflow VALUES ('2020-01-01 00:00:00', 1.0), ('2020-01-01 00:00:01', 2.0);

-- `ToGridSparse` variant: `timeSeriesResampleToGridWithStaleness`. Exercises the
-- `current_timestamp += Base::step` accumulator path.
SELECT length(timeSeriesResampleToGridWithStaleness(-9223372036854775808, 9223372036854775806, 9223372036854775807, 0)(timestamp, value)) AS len_to_grid_sparse
FROM ts_insert_overflow;

-- `InstantValue` variant: `timeSeriesInstantRateToGrid`. Same accumulator path with an
-- extra sliding-window read of `current_timestamp` inside the loop body.
SELECT length(timeSeriesInstantRateToGrid(-9223372036854775808, 9223372036854775806, 9223372036854775807, 0)(timestamp, value)) AS len_instant_rate
FROM ts_insert_overflow;

-- `Changes` variant: `timeSeriesChangesToGrid`. Exercises the per-iteration
-- `start_timestamp + i * step` path.
SELECT length(timeSeriesChangesToGrid(-9223372036854775808, 9223372036854775806, 9223372036854775807, 0)(timestamp, value)) AS len_changes
FROM ts_insert_overflow;

-- `ExtrapolatedValue` variant: `timeSeriesRateToGrid`. Same per-iteration path.
SELECT length(timeSeriesRateToGrid(-9223372036854775808, 9223372036854775806, 9223372036854775807, 0)(timestamp, value)) AS len_rate
FROM ts_insert_overflow;

-- `LinearRegression` variant: `timeSeriesDerivToGrid`. Same per-iteration path.
SELECT length(timeSeriesDerivToGrid(-9223372036854775808, 9223372036854775806, 9223372036854775807, 0)(timestamp, value)) AS len_deriv
FROM ts_insert_overflow;

DROP TABLE ts_insert_overflow;
