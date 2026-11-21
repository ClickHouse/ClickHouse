-- timeSeriesQuantilePrometheusHistogram takes the level as an argument, used by PromQL histogram_quantile with a per-step phi.

SET allow_experimental_time_series_aggregate_functions = 0;
SET allow_experimental_time_series_table = 0;
SELECT timeSeriesQuantilePrometheusHistogram(le, count, 0.5) FROM VALUES('le Float64, count UInt64', (1, 1), (inf, 2)); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

SET allow_experimental_time_series_aggregate_functions = 1;

SELECT 'same result as quantilePrometheusHistogram';
SELECT quantilePrometheusHistogram(0.35)(le, count), timeSeriesQuantilePrometheusHistogram(le, count, 0.35)
FROM VALUES('le Float64, count UInt64', (0, 6), (0.5, 11), (1, 14), (inf, 19));
SELECT quantilePrometheusHistogram(0.9)(le, count), timeSeriesQuantilePrometheusHistogram(le, count, 0.9)
FROM VALUES('le Float32, count Float64', (0.1, 1), (0.5, 3), (1, 7), (inf, 10));

SELECT 'a different level in each group';
SELECT g, timeSeriesQuantilePrometheusHistogram(le, count, level)
FROM VALUES('g UInt8, level Float64, le Float64, count UInt64',
    (1, 0.25, 0.5, 5), (1, 0.25, 1, 8), (1, 0.25, inf, 10),
    (2, 0.75, 0.5, 5), (2, 0.75, 1, 8), (2, 0.75, inf, 10),
    (3, 1, 0.5, 5), (3, 1, 1, 8), (3, 1, inf, 10))
GROUP BY g ORDER BY g;

SELECT 'a different level in each position with -ForEach';
SELECT timeSeriesQuantilePrometheusHistogramForEach([le, le, le], [count, count, count], [0.25, 0.75, 1])
FROM VALUES('le Float64, count UInt64', (0.5, 5), (1, 8), (inf, 10));

SELECT 'the state keeps the level';
SELECT timeSeriesQuantilePrometheusHistogramMerge(s)
FROM (SELECT timeSeriesQuantilePrometheusHistogramState(le, count, 0.75) AS s FROM VALUES('le Float64, count UInt64', (0.5, 5), (1, 8), (inf, 10)) GROUP BY le);
SELECT finalizeAggregation(CAST(unhex(hex(timeSeriesQuantilePrometheusHistogramState(le, count, 0.75))), 'AggregateFunction(timeSeriesQuantilePrometheusHistogram, Float64, UInt64, Float64)'))
FROM VALUES('le Float64, count UInt64', (0.5, 5), (1, 8), (inf, 10));

SELECT 'errors';
SELECT timeSeriesQuantilePrometheusHistogram(le, count, 1.5) FROM VALUES('le Float64, count UInt64', (1, 1), (inf, 2)); -- { serverError PARAMETER_OUT_OF_BOUND }
SELECT timeSeriesQuantilePrometheusHistogram(le, count, nan) FROM VALUES('le Float64, count UInt64', (1, 1), (inf, 2)); -- { serverError PARAMETER_OUT_OF_BOUND }
SELECT timeSeriesQuantilePrometheusHistogram(le, count, count / 10) FROM VALUES('le Float64, count UInt64', (1, 1), (inf, 2)); -- { serverError BAD_ARGUMENTS }
-- Merging states with different levels throws.
SELECT g, timeSeriesQuantilePrometheusHistogram(le, count, level)
FROM VALUES('g UInt8, level Float64, le Float64, count UInt64', (1, 0.25, 0.5, 5), (1, 0.25, inf, 10), (2, 0.75, 0.5, 5), (2, 0.75, inf, 10))
GROUP BY g WITH TOTALS; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesQuantilePrometheusHistogram(0.5)(le, count, 0.5) FROM VALUES('le Float64, count UInt64', (1, 1), (inf, 2)); -- { serverError AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS }
SELECT timeSeriesQuantilePrometheusHistogram(le, count) FROM VALUES('le Float64, count UInt64', (1, 1), (inf, 2)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSeriesQuantilePrometheusHistogram(le, count, '0.5') FROM VALUES('le Float64, count UInt64', (1, 1), (inf, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
