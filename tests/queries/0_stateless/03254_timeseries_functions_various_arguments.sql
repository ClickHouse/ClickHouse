CREATE TABLE ts_data(id UInt64, timestamps Array(DateTime), values Array(Float64)) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE ts_data_nullable(id UInt64, timestamp UInt32, value Nullable(Float64)) ENGINE = MergeTree() ORDER BY id;

INSERT INTO ts_data VALUES (1, [10,20], [1,2]), (2, [30,40,50], [3,4]), (3, [60], [6]), (4, [], []), (5, [80], [8,9]), (6, [100], [10]);
INSERT INTO ts_data_nullable SELECT id, timestamp, value FROM ts_data ARRAY JOIN timestamps as timestamp, arrayResize(values, length(timestamps), NULL) AS value;

SET allow_experimental_time_series_aggregate_functions = 1;

-- Fail because of rows with non-matching lengths of timestamps and values
SELECT timeSeriesResampleToGridWithStaleness(10, 120, 10, 10)(timestamps, values) FROM ts_data; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesRateToGrid(10, 120, 10, 10)(timestamps, values) FROM ts_data; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesDeltaToGrid(10, 120, 10, 10)(timestamps, values) FROM ts_data; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesInstantRateToGrid(10, 120, 10, 10)(timestamps, values) FROM ts_data; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesInstantDeltaToGrid(10, 120, 10, 10)(timestamps, values) FROM ts_data; -- {serverError BAD_ARGUMENTS}

-- Filter out invalid rows where timestamp and values arrays lengths do not match
SELECT 'staleness = 10:';
SELECT timeSeriesResampleToGridWithStaleness(10, 120, 10, 10)(timestamps, values) FROM (SELECT * FROM ts_data WHERE length(timestamps) = length(values));
SELECT timeSeriesResampleToGridWithStaleness(10, 120, 10, 10)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesResampleToGridWithStalenessIf(10, 120, 10, 10)(timestamps, values, length(timestamps) = length(values)) FROM ts_data;
SELECT timeSeriesResampleToGridWithStalenessIf(10, 120, 10, 10)(timestamps, values, toNullable(length(timestamps) = length(values))) FROM ts_data;

SELECT 'staleness = 11:';
SELECT timeSeriesResampleToGridWithStaleness(10, 120, 10, 11)(timestamps, values) FROM (SELECT * FROM ts_data WHERE length(timestamps) = length(values));
SELECT timeSeriesResampleToGridWithStaleness(10, 120, 10, 11)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesResampleToGridWithStalenessIf(10, 120, 10, 11)(timestamps, values, length(timestamps) = length(values)) FROM ts_data;
SELECT timeSeriesResampleToGridWithStalenessIf(10, 120, 10, 11)(timestamps, values, toNullable(length(timestamps) = length(values))) FROM ts_data;

SELECT 'staleness = 60:';
SELECT timeSeriesRateToGrid(10, 120, 10, 60)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesRateToGridIf(10, 120, 10, 60)(timestamps, values, toNullable(true)) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesRateToGridIf(10, 120, 10, 60)(timestamps, values, length(timestamps) = length(values)) FROM ts_data;

SELECT timeSeriesDeltaToGrid(10, 120, 10, 60)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesDeltaToGridIf(10, 120, 10, 60)(timestamps, values, length(timestamps) = length(values)) FROM ts_data;
SELECT timeSeriesDeltaToGridIf(10, 120, 10, 60)(timestamps, values, toNullable(length(timestamps) = length(values))) FROM ts_data;

SELECT timeSeriesInstantRateToGrid(10, 120, 10, 60)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesInstantRateToGridIf(10, 120, 10, 60)(timestamps, values, length(timestamps) = length(values)) FROM ts_data;
SELECT timeSeriesInstantRateToGridIf(10, 120, 10, 60)(timestamps, values, if(length(timestamps) = length(values), true, NULL)) FROM ts_data;

SELECT timeSeriesInstantDeltaToGrid(10, 120, 10, 60)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesInstantDeltaToGridIf(10, 120, 10, 60)(timestamps, values, length(timestamps) = length(values)) FROM ts_data;
SELECT timeSeriesInstantDeltaToGridIf(10, 120, 10, 60)(timestamps, values, toNullable(length(timestamps) = length(values))) FROM ts_data;

SELECT 'staleness = 61:';
SELECT timeSeriesRateToGrid(10, 120, 10, 61)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesDeltaToGrid(10, 120, 10, 61)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesInstantRateToGrid(10, 120, 10, 61)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesInstantDeltaToGrid(10, 120, 10, 61)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);


SELECT * FROM ts_data_nullable WHERE value IS NULL AND id < 5;

SELECT timeSeriesResampleToGridWithStalenessIf(15, 125, 10, 10)(timestamps, values, length(timestamps) = length(values)) FROM ts_data;

-- Test with Nullable arguments
SELECT timeSeriesResampleToGridWithStaleness(15, 125, 10, 10)(arrayResize(timestamps, arrayMin([length(timestamps), length(values)]) as min_len), arrayResize(values, min_len)) FROM ts_data;
SELECT timeSeriesResampleToGridWithStaleness(15, 125, 10, 10)(timestamp, value) FROM ts_data_nullable;
SELECT timeSeriesResampleToGridWithStalenessIf(15, 125, 10, 10)(timestamp, value, id < 5) FROM ts_data_nullable;

SELECT timeSeriesResampleToGridWithStaleness(15, 125, 10, 10)([10, 20, 30]::Array(UInt32), [1.0, 2.0, NULL]); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT timeSeriesResampleToGridWithStaleness(15, 125, 10, 10)([10, NULL, 30]::Array(Nullable(UInt32)), [1.0, 2.0, 3.0]); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- End timestamp not aligned by step
SELECT timeSeriesResampleToGridWithStaleness(100, 110, 15, 10)([89, 101, 109]::Array(UInt32), [89, 101, 109]::Array(Float32));
SELECT timeSeriesResampleToGridWithStaleness(100, 120, 15, 10)([89, 101, 109]::Array(UInt32), [89, 101, 109]::Array(Float32));
SELECT timeSeriesRateToGrid(100, 140, 15, 40)([89, 101, 109]::Array(UInt32), [89, 101, 109]::Array(Float32));
SELECT timeSeriesInstantRateToGrid(100, 140, 15, 40)([89, 101, 109]::Array(UInt32), [89, 101, 109]::Array(Float32));
SELECT timeSeriesInstantDeltaToGrid(100, 150, 15, 20)([89, 101, 109]::Array(UInt32), [89, 101, 109]::Array(Float32));

-- Start timestamp equals to end timestamp
SELECT timeSeriesRateToGrid(120, 120, 0, 40)([89, 101, 109]::Array(UInt32), [89, 101, 109]::Array(Float32));
SELECT timeSeriesInstantRateToGrid(120, 120, 0, 40)([89, 101, 109]::Array(UInt32), [89, 101, 109]::Array(Float32));
SELECT timeSeriesInstantDeltaToGrid(120, 120, 0, 20)([89, 101, 109]::Array(UInt32), [89, 101, 109]::Array(Float32));

-- First bucket doesn't have a value.
SELECT timeSeriesResampleToGridWithStaleness(105, 210, 15, 30)([110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(UInt32), [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32));
SELECT timeSeriesResampleToGridWithStaleness(105, 210, 15, 300)([110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(UInt32), [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32));
SELECT timeSeriesResampleToGridWithStaleness(90, 210, 15, 300)([110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(UInt32), [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32));

SELECT timeSeriesResampleToGridWithStaleness(100, 150, 10, 30)(toDateTime(105), [1., 2., 3.]); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT timeSeriesRateToGrid(100, 150, 10, 30)(toDateTime(105), [1., 2., 3.]); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT timeSeriesInstantDeltaToGrid(100, 150, 10, 30)(toDateTime(105), [1., 2., 3.]); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT timeSeriesResampleToGridWithStaleness(100, 150, 10, 30)(toDateTime(105), arrayJoin([1., 2., 3.]));
SELECT timeSeriesInstantRateToGrid(100, 150, 10, 30)([1, 2, 3]::Array(UInt32), 1.); --{serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT timeSeriesDeltaToGrid(100, 150, 10, 30)([1, 2, 3]::Array(UInt32), 1.); --{serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- Try to use aggregation function state in combinators with start, end, step and window parameters that are different from original parameters
-- An error should be returned
SELECT timeSeriesResampleToGridWithStalenessMerge(toNullable(60), 100, 200, 20)(
    initializeAggregation('timeSeriesResampleToGridWithStalenessState(100, 200, 20, 60)', (100 + number*10)::DateTime32, number::Float64)
) FROM numbers(5); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT timeSeriesInstantDeltaToGridMerge(toNullable(60), 100, 200, 20)(
    initializeAggregation('timeSeriesInstantDeltaToGridState(100, 200, 20, 60)', (100 + number*10)::DateTime32, number::Float64)
) FROM numbers(5); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT timeSeriesResampleToGridWithStalenessMerge(60, 100, 200, 20)(
    initializeAggregation('timeSeriesResampleToGridWithStalenessState(100, 200, 20, 60)', (100 + number*10)::DateTime32, number::Float64)
) FROM numbers(5); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- With matching parameters everything should work
SELECT timeSeriesResampleToGridWithStalenessMerge(100, 200, 20, 60)(
    initializeAggregation('timeSeriesResampleToGridWithStalenessState(100, 200, 20, 60)', (100 + number*10)::DateTime32, number::Float64)
) FROM numbers(5);
SELECT timeSeriesInstantDeltaToGridMerge(100, 200, 20, 60)(
    initializeAggregation('timeSeriesInstantDeltaToGridState(100, 200, 20, 60)', (100 + number*10)::DateTime32, number::Float64)
) FROM numbers(5);
SELECT timeSeriesResampleToGridWithStalenessMerge(100, 200, 20, 60)(
    initializeAggregation('timeSeriesResampleToGridWithStalenessState(100, 200, 20, 60)', (100 + number*10)::DateTime32, number::Float64)
) FROM numbers(5);

DROP TABLE ts_data;
DROP TABLE ts_data_nullable;
