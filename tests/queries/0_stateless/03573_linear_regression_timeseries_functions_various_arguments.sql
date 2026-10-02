CREATE TABLE ts_data(id UInt64, timestamps Array(DateTime), values Array(Float64)) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE ts_data_nullable(id UInt64, timestamp UInt32, value Nullable(Float64)) ENGINE = MergeTree() ORDER BY id;

INSERT INTO ts_data VALUES (1, [10,20], [1,2]), (2, [30,40,50], [3,4]), (3, [60], [6]), (4, [], []), (5, [80], [8,9]), (6, [100], [10]);
INSERT INTO ts_data_nullable SELECT id, timestamp, value FROM ts_data ARRAY JOIN timestamps as timestamp, arrayResize(values, length(timestamps), NULL) AS value;

SET allow_experimental_time_series_aggregate_functions = 1;

-- Fail because of rows with non-matching lengths of timestamps and values
SELECT timeSeriesDerivToGrid(10, 120, 10, 10)(timestamps, values) FROM ts_data; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesPredictLinearToGrid(10, 120, 10, 10, 60)(timestamps, values) FROM ts_data; -- {serverError BAD_ARGUMENTS}

-- Filter out invalid rows where timestamp and values arrays lengths do not match
SELECT 'staleness = 60:';
SELECT timeSeriesDerivToGrid(10, 120, 10, 60)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesDerivToGridIf(10, 120, 10, 60)(timestamps, values, length(timestamps) = length(values)) FROM ts_data;
SELECT timeSeriesDerivToGridIf(10, 120, 10, 60)(timestamps, values, toNullable(length(timestamps) = length(values))) FROM ts_data;

SELECT timeSeriesPredictLinearToGrid(10, 120, 10, 60, 60)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesPredictLinearToGridIf(10, 120, 10, 60, 60)(timestamps, values, length(timestamps) = length(values)) FROM ts_data;
SELECT timeSeriesPredictLinearToGridIf(10, 120, 10, 60, 60)(timestamps, values, toNullable(length(timestamps) = length(values))) FROM ts_data;

SELECT timeSeriesPredictLinearToGrid(10, 120, 10, 60, 60.5)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesPredictLinearToGridIf(10, 120, 10, 60, 60.5)(timestamps, values, length(timestamps) = length(values)) FROM ts_data;
SELECT timeSeriesPredictLinearToGridIf(10, 120, 10, 60, 60.5)(timestamps, values, toNullable(length(timestamps) = length(values))) FROM ts_data;

SELECT timeSeriesPredictLinearToGrid(10, 120, 10, 60, -60)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesPredictLinearToGridIf(10, 120, 10, 60, -60)(timestamps, values, length(timestamps) = length(values)) FROM ts_data;
SELECT timeSeriesPredictLinearToGridIf(10, 120, 10, 60, -60)(timestamps, values, toNullable(length(timestamps) = length(values))) FROM ts_data;

SELECT 'staleness = 61:';
SELECT timeSeriesDerivToGrid(10, 120, 10, 61)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesPredictLinearToGrid(10, 120, 10, 61, 60)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesPredictLinearToGrid(10, 120, 10, 61, 60.5)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);
SELECT timeSeriesPredictLinearToGrid(10, 120, 10, 61, -60)(timestamps, values) FROM ts_data WHERE length(timestamps) = length(values);

SELECT * FROM ts_data_nullable WHERE value IS NULL AND id < 5;

-- Test with Nullable arguments
SELECT timeSeriesResampleToGridWithStaleness(15, 125, 10, 10)(arrayResize(timestamps, arrayMin([length(timestamps), length(values)]) as min_len), arrayResize(values, min_len)) FROM ts_data;
SELECT timeSeriesResampleToGridWithStaleness(15, 125, 10, 10)(timestamp, value) FROM ts_data_nullable;
SELECT timeSeriesResampleToGridWithStalenessIf(15, 125, 10, 10)(timestamp, value, id < 5) FROM ts_data_nullable;

SELECT timeSeriesResampleToGridWithStaleness(15, 125, 10, 10)([10, 20, 30]::Array(UInt32), [1.0, 2.0, NULL]); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT timeSeriesResampleToGridWithStaleness(15, 125, 10, 10)([10, NULL, 30]::Array(Nullable(UInt32)), [1.0, 2.0, 3.0]); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- End timestamp not aligned by step
SELECT timeSeriesDerivToGrid(100, 120, 15, 20)([89, 101, 109]::Array(UInt32), [89, 101, 109]::Array(Float32));
SELECT timeSeriesPredictLinearToGrid(100, 120, 15, 20, 60)([89, 101, 109]::Array(UInt32), [89, 101, 109]::Array(Float32));

SELECT timeSeriesDerivToGrid(100, 150, 10, 30)([1, 2, 3]::Array(UInt32), 1.); --{serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT timeSeriesPredictLinearToGrid(100, 150, 10, 30, 60)([1, 2, 3]::Array(UInt32), 1.); --{serverError ILLEGAL_TYPE_OF_ARGUMENT}

DROP TABLE ts_data;
DROP TABLE ts_data_nullable;
