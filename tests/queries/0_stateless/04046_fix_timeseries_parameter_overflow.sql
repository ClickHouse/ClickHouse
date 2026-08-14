-- Test that extreme parameter values don't cause signed integer overflow when the parameters
-- of timeSeries*ToGrid functions are converted to the scale of the timestamp column.
-- A large Int64 value multiplied by the scale multiplier doesn't fit into Decimal64 and must be rejected.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=99724&sha=465742228dbb9152c5c3f98cc28f5249b27f98ab&name_0=PR&name_1=AST%20fuzzer%20%28amd_ubsan%29

SET allow_experimental_ts_to_grid_aggregate_function = 1;

CREATE TABLE ts_data_overflow (timestamp DateTime64(3, 'UTC'), value Float64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO ts_data_overflow VALUES ('2020-01-01 00:00:00.000', 1.0), ('2020-01-01 00:00:01.000', 2.0);

SELECT timeSeriesResampleToGridWithStaleness(100, 150, 9223372036854775806, 50)(timestamp, value) AS res FROM ts_data_overflow FORMAT Null; -- { serverError DECIMAL_OVERFLOW }

DROP TABLE ts_data_overflow;
