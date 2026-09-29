-- Checks the single argument form of the timeSeries*ToGrid functions, where the samples are passed
-- as an array of pairs. Every function is called twice - with two arguments and with a single argument -
-- and both calls must return the same result.

DROP TABLE IF EXISTS ts_data;

CREATE TABLE ts_data(id UInt64, timestamps Array(DateTime), values Array(Float64)) ENGINE = MergeTree() ORDER BY id;

INSERT INTO ts_data VALUES (1, [110, 120, 130, 140], [1, 1, 3, 4]), (2, [190, 200, 210, 220, 230], [5, 5, 8, 12, 13]), (3, [], []);

SET allow_experimental_time_series_aggregate_functions = 1;

SELECT 'timeSeriesRateToGrid:';
SELECT timeSeriesRateToGrid(90, 240, 15, 45)(timestamps, values) FROM ts_data;
SELECT timeSeriesRateToGrid(90, 240, 15, 45)(arrayZip(timestamps, values)) FROM ts_data;

SELECT 'timeSeriesIncreaseToGrid:';
SELECT timeSeriesIncreaseToGrid(90, 240, 15, 45)(timestamps, values) FROM ts_data;
SELECT timeSeriesIncreaseToGrid(90, 240, 15, 45)(arrayZip(timestamps, values)) FROM ts_data;

SELECT 'timeSeriesDeltaToGrid:';
SELECT timeSeriesDeltaToGrid(90, 240, 15, 45)(timestamps, values) FROM ts_data;
SELECT timeSeriesDeltaToGrid(90, 240, 15, 45)(arrayZip(timestamps, values)) FROM ts_data;

SELECT 'timeSeriesInstantRateToGrid:';
SELECT timeSeriesInstantRateToGrid(90, 240, 15, 45)(timestamps, values) FROM ts_data;
SELECT timeSeriesInstantRateToGrid(90, 240, 15, 45)(arrayZip(timestamps, values)) FROM ts_data;

SELECT 'timeSeriesInstantDeltaToGrid:';
SELECT timeSeriesInstantDeltaToGrid(90, 240, 15, 45)(timestamps, values) FROM ts_data;
SELECT timeSeriesInstantDeltaToGrid(90, 240, 15, 45)(arrayZip(timestamps, values)) FROM ts_data;

SELECT 'timeSeriesDerivToGrid:';
SELECT timeSeriesDerivToGrid(90, 240, 15, 45)(timestamps, values) FROM ts_data;
SELECT timeSeriesDerivToGrid(90, 240, 15, 45)(arrayZip(timestamps, values)) FROM ts_data;

SELECT 'timeSeriesPredictLinearToGrid:';
SELECT timeSeriesPredictLinearToGrid(90, 240, 15, 45, 60)(timestamps, values) FROM ts_data;
SELECT timeSeriesPredictLinearToGrid(90, 240, 15, 45, 60)(arrayZip(timestamps, values)) FROM ts_data;

SELECT 'timeSeriesChangesToGrid:';
SELECT timeSeriesChangesToGrid(90, 240, 15, 45)(timestamps, values) FROM ts_data;
SELECT timeSeriesChangesToGrid(90, 240, 15, 45)(arrayZip(timestamps, values)) FROM ts_data;

SELECT 'timeSeriesResetsToGrid:';
SELECT timeSeriesResetsToGrid(90, 240, 15, 45)(timestamps, values) FROM ts_data;
SELECT timeSeriesResetsToGrid(90, 240, 15, 45)(arrayZip(timestamps, values)) FROM ts_data;

SELECT 'timeSeriesResampleToGridWithStaleness:';
SELECT timeSeriesResampleToGridWithStaleness(90, 240, 15, 45)(timestamps, values) FROM ts_data;
SELECT timeSeriesResampleToGridWithStaleness(90, 240, 15, 45)(arrayZip(timestamps, values)) FROM ts_data;

SELECT 'Grouping by id:';
SELECT id, timeSeriesResampleToGridWithStaleness(90, 240, 15, 45)(timestamps, values) FROM ts_data GROUP BY id ORDER BY id;
SELECT id, timeSeriesResampleToGridWithStaleness(90, 240, 15, 45)(arrayZip(timestamps, values)) FROM ts_data GROUP BY id ORDER BY id;

SELECT 'The -If combinator:';
SELECT timeSeriesRateToGridIf(90, 240, 15, 45)(timestamps, values, id = 2) FROM ts_data;
SELECT timeSeriesRateToGridIf(90, 240, 15, 45)(arrayZip(timestamps, values), id = 2) FROM ts_data;
SELECT timeSeriesDeltaToGridIf(90, 240, 15, 45)(timestamps, values, toNullable(id != 1)) FROM ts_data;
SELECT timeSeriesDeltaToGridIf(90, 240, 15, 45)(arrayZip(timestamps, values), toNullable(id != 1)) FROM ts_data;

DROP TABLE IF EXISTS ts_data_64;

CREATE TABLE ts_data_64(id UInt64, samples Array(Tuple(DateTime64(3, 'UTC'), Float32))) ENGINE = MergeTree() ORDER BY id;

INSERT INTO ts_data_64 SELECT id, arrayZip(arrayMap(timestamp -> toDateTime64(timestamp, 3, 'UTC'), timestamps), arrayMap(value -> toFloat32(value), values)) FROM ts_data;

SELECT 'DateTime64 timestamps and Float32 values:';
SELECT timeSeriesRateToGrid(90, 240, 15, 45)(arrayMap(sample -> sample.1, samples), arrayMap(sample -> sample.2, samples)) FROM ts_data_64;
SELECT timeSeriesRateToGrid(90, 240, 15, 45)(samples) FROM ts_data_64;
SELECT timeSeriesResampleToGridWithStaleness(90, 240, 15, 45)(arrayMap(sample -> sample.1, samples), arrayMap(sample -> sample.2, samples)) FROM ts_data_64;
SELECT timeSeriesResampleToGridWithStaleness(90, 240, 15, 45)(samples) FROM ts_data_64;

SELECT 'A single row of unsorted samples:';
SELECT timeSeriesResampleToGridWithStaleness(90, 240, 15, 45)([(140, 4.), (110, 1.), (130, 3.), (120, 1.)]::Array(Tuple(DateTime, Float64)));

SELECT 'Errors:';
SELECT timeSeriesRateToGrid(90, 240, 15, 45)(timestamps) FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesRateToGrid(90, 240, 15, 45)(arrayZip(timestamps, values, values)) FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesRateToGrid(90, 240, 15, 45)(arrayZip(arrayMap(timestamp -> toString(timestamp), timestamps), values)) FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesRateToGrid(90, 240, 15, 45)(arrayZip(timestamps, arrayMap(value -> toString(value), values))) FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE ts_data_64;
DROP TABLE ts_data;
