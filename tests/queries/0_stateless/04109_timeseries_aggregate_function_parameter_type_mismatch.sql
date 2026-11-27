-- Aggregate function parameters are part of the stored state type, so differently-spelled literals
-- (UInt64 integer vs DecimalField<Decimal64> from a DateTime64 cast) produce non-equal AggregateFunction
-- types even when the numeric values agree. `INSERT ... SELECT` between such types therefore fails.
--
-- This matches the behaviour of every non-timeseries parameterised aggregate
-- (e.g. quantile(0.5) vs quantile(toFloat32(0.5))).
-- Users must spell parameters consistently on the CREATE TABLE and INSERT sides.

SET allow_experimental_ts_to_grid_aggregate_function = 1;

DROP TABLE IF EXISTS ts_param_test;

-- Integer timestamp parameters in the column declaration (Field tag: UInt64).
CREATE TABLE ts_param_test
(
    k UInt64,
    agg AggregateFunction(
        timeSeriesRateToGrid(1704067200, 1704067260, 10, 50),
        Array(DateTime64(3, 'UTC')),
        Array(Float64)
    )
) ENGINE = AggregatingMergeTree() ORDER BY k;

-- DateTime64-cast params produce a Decimal64-tagged state type; the column has UInt64 tags, so the INSERT cast is rejected.
INSERT INTO ts_param_test
SELECT 1, timeSeriesRateToGridState(
    '2024-01-01 00:00:00'::DateTime64(3, 'UTC'),
    '2024-01-01 00:01:00'::DateTime64(3, 'UTC'),
    10, 50
)(
    [toDateTime64('2024-01-01 00:00:10', 3, 'UTC'),
     toDateTime64('2024-01-01 00:00:20', 3, 'UTC')]::Array(DateTime64(3, 'UTC')),
    [1.0, 2.0]::Array(Float64)
); -- { serverError CANNOT_CONVERT_TYPE }

-- Consistent spelling works: integer literals on both sides.
INSERT INTO ts_param_test
SELECT 1, timeSeriesRateToGridState(1704067200, 1704067260, 10, 50)(
    [toDateTime64('2024-01-01 00:00:10', 3, 'UTC'),
     toDateTime64('2024-01-01 00:00:20', 3, 'UTC')]::Array(DateTime64(3, 'UTC')),
    [1.0, 2.0]::Array(Float64)
);

SELECT k, finalizeAggregation(agg) FROM ts_param_test ORDER BY k;

DROP TABLE ts_param_test;
