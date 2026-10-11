-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET enable_time_series_aggregate_functions = 0;
SET enable_time_series_table = 0;
SELECT timeSeriesAvgOverGroup(k, [x]) FROM values('k UInt8, x Float64', (1, 1)); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

SET enable_time_series_aggregate_functions = 1;

SELECT '-- timeSeriesAvgOverGroup';
SELECT avgForEach(v), timeSeriesAvgOverGroup(k, v) FROM values('k UInt8, v Array(Float64)', (1, [1e308, -1e308]), (2, [1e308, -1e308]), (3, [1e308, -1e308]));
SELECT avgForEach(v), timeSeriesAvgOverGroup(k, v) FROM values('k UInt8, v Array(Float64)', (1, [1]), (2, [1e100]), (3, [1]), (4, [-1e100]));
SELECT timeSeriesAvgOverGroup(k, [x]) FROM values('k UInt8, x Float64', (1, inf), (2, 1));
SELECT timeSeriesAvgOverGroup(k, [x]) FROM values('k UInt8, x Float64', (1, 1e308), (2, 1e308), (3, inf));
SELECT timeSeriesAvgOverGroup(k, [x]) FROM values('k UInt8, x Float64', (1, inf), (2, inf), (3, -inf));
SELECT timeSeriesAvgOverGroup(k, [x]) FROM values('k UInt8, x Float64', (1, nan), (2, 1));
SELECT timeSeriesAvgOverGroup(k, v) FROM values('k String, v Array(Nullable(Float64))', ('a', [NULL, 3, NULL]), ('b', [1e308, NULL, NULL]), ('c', [1e308, 5, NULL]));
SELECT timeSeriesAvgOverGroup(k, v) FROM values('k UInt8, v Array(Float64)', (1, [1])) WHERE k > 1;
SELECT timeSeriesAvgOverGroup(k, v) FROM values('k Nullable(UInt8), v Array(Float64)', (NULL, [1e308]), (1, [2]), (2, [4]));
-- A NULL key keeps its row and sorts last, so it gives the same result as the largest key.
SELECT timeSeriesAvgOverGroup(k, [x]) FROM values('k Nullable(Int8), x Float64', (NULL, -1e308), (2, 9.988465674311579e307), (1, 1e308), (3, -9.988465674311579e307));
SELECT finalizeAggregation(CAST(unhex(hex(timeSeriesAvgOverGroupState(k, v))), 'AggregateFunction(timeSeriesAvgOverGroup, Nullable(UInt8), Array(Float64))'))
FROM values('k Nullable(UInt8), v Array(Float64)', (NULL, [1e308]), (1, [2]), (2, [4]));
SELECT timeSeriesAvgOverGroup(k) FROM values('k UInt8', 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSeriesAvgOverGroup(k, x) FROM values('k UInt8, x Float64', (1, 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesAvgOverGroup(k, [toFloat32(1)]) FROM values('k UInt8', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesAvgOverGroup(k, [1.]) FROM (SELECT sumState(1) AS k); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesAvgOverGroup(k, v) FROM values('k UInt8, v Array(Float64)', (1, [1]), (2, [1, 2])); -- { serverError BAD_ARGUMENTS }

SELECT '-- the order of the keys sets the rounding, the order of the rows does not';
-- Prometheus returns -4.9896007738368e291 and 4.9896007738368e291 for these two orders of the values.
SELECT timeSeriesAvgOverGroup(k, [x]), timeSeriesAvgOverGroup(-k, [x])
FROM values('k Int8, x Float64', (4, -1e308), (2, 9.988465674311579e307), (1, 1e308), (3, -9.988465674311579e307));
SELECT timeSeriesAvgOverGroup(k, [x]), timeSeriesAvgOverGroup(-k, [x])
FROM values('k Int8, x Float64', (1, 1e308), (3, -9.988465674311579e307), (4, -1e308), (2, 9.988465674311579e307));
-- Rows with equal keys are ordered by their values.
SELECT uniqExact(r), any(r) FROM (
    SELECT n, timeSeriesAvgOverGroup(1, [x]) AS r
    FROM (SELECT number % 30 AS n, [1e308, 9.988465674311579e307, -9.988465674311579e307, -1e308][intDiv(number, 30) + 1] AS x FROM numbers(120) ORDER BY cityHash64(number))
    GROUP BY n SETTINGS max_threads = 4, max_block_size = 7);

SELECT '-- merge of states';
-- Only the second state overflows: the merge gives the one-pass result.
SELECT timeSeriesAvgOverGroupMerge(s) FROM (
    SELECT timeSeriesAvgOverGroupState(k, [x]) AS s
    FROM values('part UInt8, k UInt8, x Float64', (1, 1, 1e308), (2, 2, -1e308), (2, 3, -9.988465674311579e307), (2, 4, 9.988465674311579e307))
    GROUP BY part);
SELECT timeSeriesAvgOverGroup(k, [x]) FROM values('k UInt8, x Float64', (1, 1e308), (2, -1e308), (3, -9.988465674311579e307), (4, 9.988465674311579e307));
-- 1000 rows whose average depends on their order: Prometheus returns -9.976931348621681e302 for the order of the keys.
SELECT timeSeriesAvgOverGroup(number, [[1e308, 9.988465674311579e307, -9.988465674311579e307, -1e308][number % 4 + 1] * (1 - (number % 7) / 100)])
FROM numbers(1000) SETTINGS max_threads = 1;
SELECT timeSeriesAvgOverGroup(number, [[1e308, 9.988465674311579e307, -9.988465674311579e307, -1e308][number % 4 + 1] * (1 - (number % 7) / 100)])
FROM (SELECT number FROM numbers(1000) ORDER BY cityHash64(number)) SETTINGS max_threads = 8, max_block_size = 10;
SELECT timeSeriesAvgOverGroupMerge(s) FROM (
    SELECT timeSeriesAvgOverGroupState(number, [[1e308, 9.988465674311579e307, -9.988465674311579e307, -1e308][number % 4 + 1] * (1 - (number % 7) / 100)]) AS s
    FROM numbers(1000) GROUP BY number % 13) SETTINGS max_threads = 8, max_block_size = 10;
SELECT finalizeAggregation(CAST(unhex(hex(timeSeriesAvgOverGroupState(k, v))), 'AggregateFunction(timeSeriesAvgOverGroup, Array(Tuple(String, String)), Array(Nullable(Float64)))'))
FROM values('k Array(Tuple(String, String)), v Array(Nullable(Float64))', ([('a', '1')], [1e308, NULL]), ([('a', '2')], [9.988465674311579e307, 3]), ([('b', '1')], [-9.988465674311579e307, NULL]), ([], [-1e308, 5]));
SELECT finalizeAggregation(CAST(unhex('0105'), 'AggregateFunction(timeSeriesAvgOverGroup, UInt8, Array(Float64))')); -- { serverError CANNOT_READ_ALL_DATA }

SELECT '-- PromQL avg';

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET enable_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

-- The data of the `avg` tests of Prometheus (promql/promqltest/testdata/aggregators.test), and series whose average depends on their order.
-- The rows are inserted in reverse order of the labels: Prometheus averages series in the order of their labels.
INSERT INTO prometheus (metric_name, tags, samples)
SELECT 'data', map('test', t, 'point', p), [(toDateTime64(0, 3), v)] FROM values('t String, p String, v Float64',
    ('cancel', 'f', -1e308), ('cancel', 'e', 1), ('cancel', 'd', -1e308), ('cancel', 'c', 1e308), ('cancel', 'b', 1), ('cancel', 'a', 1e308),
    ('order3', 'd', -9.988465674311579e307), ('order3', 'c', -1e308), ('order3', 'b', 1e308), ('order3', 'a', 9.988465674311579e307),
    ('order2', 'd', 1e308), ('order2', 'c', 9.988465674311579e307), ('order2', 'b', -9.988465674311579e307), ('order2', 'a', -1e308),
    ('order', 'd', -1e308), ('order', 'c', -9.988465674311579e307), ('order', 'b', 9.988465674311579e307), ('order', 'a', 1e308),
    ('kahan', 'd', -1e100), ('kahan', 'c', 1e100), ('kahan', 'b', 8), ('kahan', 'a', 2));
INSERT INTO prometheus (metric_name, tags, samples)
SELECT 'data', map('test', t, 'point', p), [(toDateTime64(0, 3), v)] FROM values('t String, p String, v Float64',
    ('bigzero', 'd', 9.988465674311579e+307), ('bigzero', 'c', 9.988465674311579e+307), ('bigzero', 'b', -9.988465674311579e+307), ('bigzero', 'a', -9.988465674311579e+307),
    ('-big', 'd', -9.988465674311579e+307), ('-big', 'c', -9.988465674311579e+307), ('-big', 'b', -9.988465674311579e+307), ('-big', 'a', -9.988465674311579e+307),
    ('big', 'd', 9.988465674311579e+307), ('big', 'c', 9.988465674311579e+307), ('big', 'b', 9.988465674311579e+307), ('big', 'a', 9.988465674311579e+307),
    ('nan', 'c', inf), ('nan', 'b', 0), ('nan', 'a', -inf),
    ('inf', 'c', 0), ('inf', 'b', inf), ('inf', 'a', 0),
    ('ten', 'c', 12), ('ten', 'b', 10), ('ten', 'a', 8));

SELECT tags, value FROM prometheusQuery('prometheus', 'avg by (test) (data)', 60) ORDER BY tags SETTINGS max_threads = 1;
SELECT tags, value FROM prometheusQuery('prometheus', 'avg by (test) (data)', 60) ORDER BY tags SETTINGS max_threads = 8, max_block_size = 3;
-- A fused pair of aggregations over the same argument.
SELECT tags, value FROM prometheusQuery('prometheus', 'avg by (test) (data) - max by (test) (data)', 60) ORDER BY tags;

-- Two series over a range: the sum overflows at the second and third steps only.
INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 1e308), (toDateTime64(120, 3), 1.5e308), (toDateTime64(130, 3), 4)]),
    ('m', map('host', 'h2'), [(toDateTime64(100, 3), 3), (toDateTime64(110, 3), 1e308), (toDateTime64(120, 3), 1e308), (toDateTime64(130, 3), -1e308)]);

SELECT * FROM prometheusQueryRange('prometheus', 'avg(m)', 100, 130, 10);

-- Four series over a range, in a different order of the values at each step; `s="b"` has no value at the last step.
INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('o', map('s', 'd'), [(toDateTime64(1000, 3), -1e308), (toDateTime64(1400, 3), 1e308), (toDateTime64(1800, 3), -9.988465674311579e307), (toDateTime64(2200, 3), -9.988465674311579e307)]),
    ('o', map('s', 'c'), [(toDateTime64(1000, 3), -9.988465674311579e307), (toDateTime64(1400, 3), 9.988465674311579e307), (toDateTime64(1800, 3), -1e308), (toDateTime64(2200, 3), -1e308)]),
    ('o', map('s', 'b'), [(toDateTime64(1000, 3), 9.988465674311579e307), (toDateTime64(1400, 3), -9.988465674311579e307), (toDateTime64(1800, 3), 1e308)]),
    ('o', map('s', 'a'), [(toDateTime64(1000, 3), 1e308), (toDateTime64(1400, 3), -1e308), (toDateTime64(1800, 3), 9.988465674311579e307), (toDateTime64(2200, 3), 1e308)]);

SELECT * FROM prometheusQueryRange('prometheus', 'avg(o)', 1000, 2200, 400) SETTINGS max_threads = 1;
SELECT * FROM prometheusQueryRange('prometheus', 'avg(o)', 1000, 2200, 400) SETTINGS max_threads = 8, max_block_size = 1;

DROP TABLE prometheus;
