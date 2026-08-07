CREATE TABLE ts_data(id UInt64, timestamps Array(DateTime), values Array(Float64)) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE ts_data_nullable(id UInt64, timestamp UInt32, value Nullable(Float64)) ENGINE = MergeTree() ORDER BY id;

INSERT INTO ts_data VALUES (1, [10,20, 30], [1,5,5]), (2, [40,50,60], [1,3]), (3, [70], [2]), (4, [], []), (5, [80], [8,9]), (6, [100], [10]);
INSERT INTO ts_data_nullable SELECT id, timestamp, value FROM ts_data ARRAY JOIN timestamps as timestamp, arrayResize(values, length(timestamps), NULL) AS value;

SET allow_experimental_time_series_aggregate_functions = 1;

-- Fail because of rows with non-matching lengths of timestamps and values
SELECT timeSeriesChangesToGrid(10, 120, 10, 10)(timestamps, values) FROM ts_data; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesResetsToGrid(10, 120, 10, 10)(timestamps, values) FROM ts_data; -- {serverError BAD_ARGUMENTS}

-- Filter out invalid rows where timestamp and values arrays lengths do not match
SELECT timeSeriesChangesToGrid(10, 120, 10, 70)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesChangesToGridIf(10, 120, 10, 70)(timestamps, values, length(timestamps) = length(values)) FROM ts_data;
SELECT timeSeriesChangesToGridIf(10, 120, 10, 70)(timestamps, values, toNullable(length(timestamps) = length(values))) FROM ts_data;

SELECT timeSeriesResetsToGrid(10, 120, 10, 70)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesResetsToGridIf(10, 120, 10, 70)(timestamps, values, length(timestamps) = length(values)) FROM ts_data;
SELECT timeSeriesResetsToGridIf(10, 120, 10, 70)(timestamps, values, toNullable(length(timestamps) = length(values))) FROM ts_data;


SELECT * FROM ts_data_nullable WHERE value IS NULL AND id < 5;

SELECT timeSeriesResampleToGridWithStalenessIf(15, 125, 10, 10)(timestamps, values, length(timestamps) = length(values)) FROM ts_data;

-- Test with Nullable arguments
SELECT timeSeriesChangesToGrid(15, 125, 10, 20)(arrayResize(timestamps, arrayMin([length(timestamps), length(values)]) as min_len), arrayResize(values, min_len)) FROM ts_data;
SELECT timeSeriesChangesToGrid(15, 125, 10, 20)(timestamp, value) FROM ts_data_nullable;
SELECT timeSeriesChangesToGridIf(15, 125, 10, 20)(timestamp, value, id < 5) FROM ts_data_nullable;

SELECT timeSeriesResetsToGrid(15, 125, 10, 20)(arrayResize(timestamps, arrayMin([length(timestamps), length(values)]) as min_len), arrayResize(values, min_len)) FROM ts_data;
SELECT timeSeriesResetsToGrid(15, 125, 10, 20)(timestamp, value) FROM ts_data_nullable;
SELECT timeSeriesResetsToGridIf(15, 125, 10, 20)(timestamp, value, id < 5) FROM ts_data_nullable;

SELECT timeSeriesChangesToGrid(15, 125, 10, 20)([10, 20, 30]::Array(UInt32), [1.0, 2.0, NULL]); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT timeSeriesChangesToGrid(15, 125, 10, 20)([10, NULL, 30]::Array(Nullable(UInt32)), [1.0, 2.0, 3.0]); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT timeSeriesResetsToGrid(15, 125, 10, 20)([10, 20, 30]::Array(UInt32), [1.0, 2.0, NULL]); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT timeSeriesResetsToGrid(15, 125, 10, 20)([10, NULL, 30]::Array(Nullable(UInt32)), [1.0, 2.0, 3.0]); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- End timestamp not aligned by step
SELECT timeSeriesChangesToGrid(100, 140, 15, 40)([89, 101, 109]::Array(UInt32), [89, 101, 99]::Array(Float32));
SELECT timeSeriesResetsToGrid(100, 140, 15, 40)([89, 101, 109]::Array(UInt32), [89, 101, 99]::Array(Float32));

-- Start timestamp equals to end timestamp
SELECT timeSeriesChangesToGrid(120, 120, 0, 40)([89, 101, 109]::Array(UInt32), [89, 101, 99]::Array(Float32));
SELECT timeSeriesResetsToGrid(120, 120, 0, 40)([89, 101, 109]::Array(UInt32), [89, 101, 99]::Array(Float32));

SELECT timeSeriesChangesToGrid(100, 150, 10, 30)(toDateTime(105), [1., 2., 3.]); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT timeSeriesChangesToGrid(100, 150, 10, 30)([1, 2, 3]::Array(UInt32), 1.); --{serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT timeSeriesResetsToGrid(100, 150, 10, 30)(toDateTime(105), [1., 2., 3.]); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT timeSeriesResetsToGrid(100, 150, 10, 30)([1, 2, 3]::Array(UInt32), 1.); --{serverError ILLEGAL_TYPE_OF_ARGUMENT}

DROP TABLE ts_data;
DROP TABLE ts_data_nullable;
