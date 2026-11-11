-- Regression test: timeseries aggregate functions used with `initializeAggregation`
-- and `AggregatingMergeTree` failed under parallel replicas because the factory's
-- `extractIntParameter` did not accept `Decimal64` parameters produced by the
-- aggregate function's own parameter serialization.

SET allow_experimental_ts_to_grid_aggregate_function = 1;

CREATE TABLE ts_data (timestamp DateTime('UTC'), value Float64) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO ts_data VALUES
    ('1970-01-01 00:01:41', 10101),
    ('1970-01-01 00:01:47', 10107),
    ('1970-01-01 00:02:00', 10120),
    ('1970-01-01 00:02:07', 10127),
    ('1970-01-01 00:02:15', 10135),
    ('1970-01-01 00:02:31', 10151);

-- Create an AggregatingMergeTree table with a timeseries aggregate state column
CREATE TABLE ts_agg (
    k UInt64,
    agg AggregateFunction(timeSeriesResampleToGridWithStaleness(100, 200, 10, 15), DateTime('UTC'), Float64)
) ENGINE = AggregatingMergeTree() ORDER BY k;

-- This INSERT uses `initializeAggregation` which re-creates the aggregate function
-- from stored Decimal64 parameters. Under parallel replicas this was sent to a
-- replica that called `extractIntParameter` which did not handle Decimal64.
INSERT INTO ts_agg
    SELECT toUnixTimestamp(timestamp) % 2 AS k,
           initializeAggregation('timeSeriesResampleToGridWithStalenessState(100, 200, 10, 15)', timestamp, value)
    FROM ts_data;

SELECT k, finalizeAggregation(agg) FROM ts_agg FINAL ORDER BY k;

-- Also verify -Merge combinator works (same reconstruction path)
SELECT timeSeriesResampleToGridWithStalenessMerge(100, 200, 10, 15)(agg) FROM ts_agg;
