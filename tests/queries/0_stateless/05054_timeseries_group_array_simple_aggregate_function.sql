SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts_series;

CREATE TABLE ts_series
(
    id UInt64,
    samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3, 'UTC'), Float64)))
)
ENGINE = AggregatingMergeTree ORDER BY id;

INSERT INTO ts_series VALUES (1, [('2026-08-28 10:00:00.000', 41.), ('2026-08-28 10:00:15.000', 42.)]);
INSERT INTO ts_series VALUES (1, [('2026-08-28 10:00:15.000', 42.), ('2026-08-28 10:00:30.000', 43.)]);
INSERT INTO ts_series VALUES (2, [('2026-08-28 11:00:00.000', 1.)]);

SELECT 'SELECT FINAL:';
SELECT id, samples FROM ts_series FINAL ORDER BY id;

SELECT 'After OPTIMIZE FINAL:';
OPTIMIZE TABLE ts_series FINAL;
SELECT id, samples FROM ts_series ORDER BY id;

SELECT 'Aggregation with the single argument form:';
SELECT id, timeSeriesGroupArray(samples) FROM ts_series GROUP BY id ORDER BY id;

SELECT 'SimpleState combinator:';
SELECT toTypeName(timeSeriesGroupArraySimpleState(timestamp, value))
FROM (SELECT '2026-08-28 10:00:00.000'::DateTime64(3, 'UTC') AS timestamp, 42.::Float64 AS value);

SELECT timeSeriesGroupArray([1., 2.]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE ts_series;
