-- Regression test for STID 4811-4df3:
-- UBSan signed integer overflow report in `doInsertResultInto` of the `timeSeries*ToGrid`
-- aggregate function family.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=3de10bc76f6db2d41c599e848a03ce1ff4928827&name_0=MasterCI&name_1=Stress%20test%20%28experimental%2C%20serverfuzz%2C%20arm_ubsan%29
--
-- After the `bucketCount`, `bucketIndexForTimestamp` and `timestampAtIndex` fixes landed,
-- the AST fuzzer found one more overflow: the sliding-window expiry check
--   `samples_in_window.front().first + Base::window <= current_timestamp`
-- (and its negated `... + Base::window > current_timestamp` form) signed-overflows
-- `TimestampType` when a real, positive sample timestamp is added to a `window` near
-- `INT64_MAX`. The sample survives the (`NO_SANITIZE_UNDEFINED`) `add` filter because
-- `start_timestamp` is `INT64_MIN`, lands in a bucket, and then trips UBSan during the
-- window-expiry comparison.
--
-- The fix adds `Base::isWithinWindow`, which performs `sample_timestamp + window` in
-- 128-bit arithmetic before comparing, and routes every window-expiry check in the
-- `Changes`, `ExtrapolatedValue`, `LinearRegression`, `InstantValue` and `ToGridSparse`
-- implementations through it.
--
-- The grid `[INT64_MIN, -1, INT64_MAX - 1]` (`start = INT64_MIN`, `end = INT64_MAX - 1`,
-- `step = INT64_MAX`, `bucket_count = 3`) is paired with `window = INT64_MAX`. Each query
-- covers one public function per affected internal implementation.

SET allow_experimental_ts_to_grid_aggregate_function = 1;
SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts_window_overflow;
CREATE TABLE ts_window_overflow (timestamp DateTime64(0) NOT NULL, value Float64 NOT NULL)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ts_window_overflow VALUES ('2020-01-01 00:00:00', 1.0), ('2020-01-01 00:00:01', 2.0);

-- `ExtrapolatedValue` variant: `timeSeriesRateToGrid`.
SELECT length(timeSeriesRateToGrid(-9223372036854775808, 9223372036854775806, 9223372036854775807, 9223372036854775807)(timestamp, value)) AS len_rate
FROM ts_window_overflow;

-- `Changes` variant: `timeSeriesChangesToGrid`.
SELECT length(timeSeriesChangesToGrid(-9223372036854775808, 9223372036854775806, 9223372036854775807, 9223372036854775807)(timestamp, value)) AS len_changes
FROM ts_window_overflow;

-- `LinearRegression` variant: `timeSeriesDerivToGrid`.
SELECT length(timeSeriesDerivToGrid(-9223372036854775808, 9223372036854775806, 9223372036854775807, 9223372036854775807)(timestamp, value)) AS len_deriv
FROM ts_window_overflow;

-- `InstantValue` variant: `timeSeriesInstantRateToGrid`.
SELECT length(timeSeriesInstantRateToGrid(-9223372036854775808, 9223372036854775806, 9223372036854775807, 9223372036854775807)(timestamp, value)) AS len_instant_rate
FROM ts_window_overflow;

-- `ToGridSparse` variant: `timeSeriesResampleToGridWithStaleness`.
SELECT length(timeSeriesResampleToGridWithStaleness(-9223372036854775808, 9223372036854775806, 9223372036854775807, 9223372036854775807)(timestamp, value)) AS len_to_grid_sparse
FROM ts_window_overflow;

DROP TABLE ts_window_overflow;
