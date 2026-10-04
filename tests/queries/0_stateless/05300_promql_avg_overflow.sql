-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET enable_time_series_aggregate_functions = 0;
SET enable_time_series_table = 0;
SELECT timeSeriesAvgOverGroup(x) FROM values('x Float64', 1); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

SET enable_time_series_aggregate_functions = 1;

SELECT '-- timeSeriesAvgOverGroup';
SELECT avg(x), timeSeriesAvgOverGroup(x) FROM values('x Float64', 1e308, 1e308);
SELECT avg(x), timeSeriesAvgOverGroup(x) FROM values('x Float64', -1e308, -1e308, -1e308);
SELECT avg(x), timeSeriesAvgOverGroup(x) FROM values('x Float64', 1e308, 1e308, -1e308, -1e308);
SELECT avg(x), timeSeriesAvgOverGroup(x) FROM values('x Float64', 1, 1e100, 1, -1e100);
SELECT timeSeriesAvgOverGroup(x) FROM values('x Float64', inf, 1);
SELECT timeSeriesAvgOverGroup(x) FROM values('x Float64', 1e308, 1e308, inf);
SELECT timeSeriesAvgOverGroup(x) FROM values('x Float64', inf, inf, -inf);
SELECT timeSeriesAvgOverGroup(x) FROM values('x Float64', 1e308, 1e308, -inf, inf);
SELECT timeSeriesAvgOverGroup(x) FROM values('x Float64', nan, 1);
SELECT timeSeriesAvgOverGroup(toFloat64(number)), timeSeriesAvgOverGroup(toFloat64(number) - 20) FROM numbers(10);
SELECT timeSeriesAvgOverGroup(number) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesAvgOverGroup(x) FROM values('x Float64', 1) WHERE x > 1;
SELECT timeSeriesAvgOverGroup(x) FROM values('x Nullable(Float64)', NULL, 3, NULL, 5);
SELECT timeSeriesAvgOverGroupForEach(x) FROM values('x Array(Nullable(Float64))', [1e308, 1, NULL], [1e308, 3, NULL]);

SELECT '-- merge of states';
SELECT timeSeriesAvgOverGroupMerge(s) FROM (SELECT timeSeriesAvgOverGroupState(x) AS s FROM values('k UInt8, x Float64', (1, 1e308), (2, 1e308)) GROUP BY k);
SELECT timeSeriesAvgOverGroupMerge(s) FROM (SELECT timeSeriesAvgOverGroupState(x) AS s FROM values('k UInt8, x Float64', (1, 1e308), (1, 1e308), (2, 0), (2, 0)) GROUP BY k);
SELECT timeSeriesAvgOverGroupMerge(s) FROM (SELECT timeSeriesAvgOverGroupState(x) AS s FROM values('k UInt8, x Float64', (1, 1), (1, 1e100), (2, 1), (2, -1e100)) GROUP BY k);
SELECT timeSeriesAvgOverGroupMerge(s) FROM (SELECT timeSeriesAvgOverGroupState(x) AS s FROM values('k UInt8, x Float64', (1, inf), (2, 1e308), (2, 1e308)) GROUP BY k);
SELECT abs(timeSeriesAvgOverGroup(1e308) / 1e308 - 1) < 1e-12 FROM numbers(100000) SETTINGS max_threads = 4, max_block_size = 1000;
SELECT finalizeAggregation(CAST(unhex(hex(timeSeriesAvgOverGroupState(x))), 'AggregateFunction(timeSeriesAvgOverGroup, Float64)')) FROM values('x Float64', 1e308, 1e308, 4e307);

SELECT '-- PromQL avg';

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET enable_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

-- The data of the `avg` tests of Prometheus (promql/promqltest/testdata/aggregators.test).
INSERT INTO prometheus (metric_name, tags, samples)
SELECT 'data', map('test', t, 'point', p), [(toDateTime64(0, 3), v)] FROM values('t String, p String, v Float64',
    ('ten', 'a', 8), ('ten', 'b', 10), ('ten', 'c', 12),
    ('inf', 'a', 0), ('inf', 'b', inf), ('inf', 'c', 0),
    ('nan', 'a', -inf), ('nan', 'b', 0), ('nan', 'c', inf),
    ('big', 'a', 9.988465674311579e+307), ('big', 'b', 9.988465674311579e+307), ('big', 'c', 9.988465674311579e+307), ('big', 'd', 9.988465674311579e+307),
    ('-big', 'a', -9.988465674311579e+307), ('-big', 'b', -9.988465674311579e+307), ('-big', 'c', -9.988465674311579e+307), ('-big', 'd', -9.988465674311579e+307),
    ('bigzero', 'a', -9.988465674311579e+307), ('bigzero', 'b', -9.988465674311579e+307), ('bigzero', 'c', 9.988465674311579e+307), ('bigzero', 'd', 9.988465674311579e+307),
    ('kahan', 'a', 2), ('kahan', 'b', 8), ('kahan', 'c', 1e100), ('kahan', 'd', -1e100));

SELECT tags, value FROM prometheusQuery('prometheus', 'avg by (test) (data)', 60) ORDER BY tags;
-- A fused pair of aggregations over the same argument.
SELECT tags, value FROM prometheusQuery('prometheus', 'avg by (test) (data) - max by (test) (data)', 60) ORDER BY tags;

-- Two series over a range: the sum overflows at the second and third steps only.
INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 1e308), (toDateTime64(120, 3), 1.5e308), (toDateTime64(130, 3), 4)]),
    ('m', map('host', 'h2'), [(toDateTime64(100, 3), 3), (toDateTime64(110, 3), 1e308), (toDateTime64(120, 3), 1e308), (toDateTime64(130, 3), -1e308)]);

SELECT * FROM prometheusQueryRange('prometheus', 'avg(m)', 100, 130, 10);

DROP TABLE prometheus;
