-- Tags: no-fasttest
-- no-fasttest: ANTLR4 support is disabled in the fast-test build, and the PromQL grammar needs it.

-- Test: regression coverage for the two cross-slice review findings.
--
-- 1. `last_over_time` over a subquery on a histogram-enabled storage used to return the argument's
--    inner grid unchanged (the HISTOGRAM_GRID arm of applyFunctionOverRange passed it through,
--    assuming the grid was already resampled). An instant query threw LOGICAL_ERROR
--    ("Cannot finalize expression ... has type RANGE_VECTOR"), and a range query silently returned
--    the subquery's raw matrix on the inner step grid. Now each arm of the combined grid is
--    resampled onto the aggregation grid via `timeSeriesFromGrid`, mirroring the VECTOR_GRID arm.
--
-- 2. `histogram_count` (and histogram_sum/avg/stddev/stdvar) over a selector matching float-only
--    series used to keep such series as all-NULL rows, which are invisible in the output but still
--    counted by the duplicate-series check in `dropMetricName`: a float-only series colliding with
--    a histogram series after the `__name__` drop failed the whole query. Upstream skips series
--    without histogram samples, so the applier now filters `WHERE has(sample_kinds, 1)` exactly
--    like the histogram_quantile/histogram_fraction appliers.
--
-- NOTE: this reference file was hand-computed through the verified algorithms and converter semantics.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_nh_lot;
CREATE TABLE ts_nh_lot ENGINE = TimeSeries SETTINGS store_native_histograms = 1;

-- Native-histogram series nh{job='sub'}: custom buckets [1,2,4], samples at t=60 (count 4, sum 10)
-- and t=120 (count 8, sum 21).
INSERT INTO ts_nh_lot (metric_name, tags, histograms) VALUES
    ('nh', map('job', 'sub'), [
        (toDateTime64(60, 3), 0, -53, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [1., 2., 4.]),
        (toDateTime64(120, 3), 0, -53, 0., 8., 21., 0., [(0, 3)], [0., 2., 6.], [], [], [1., 2., 4.])]);

-- Float series f{job='sub'}: the subquery resample must work for the float arm as well (the bug
-- threw for every series on a histogram-enabled storage, not just histogram-carrying ones).
INSERT INTO ts_nh_lot (metric_name, tags, time_series) VALUES
    ('f', map('job', 'sub'), [(toDateTime64(60, 3), 1.5), (toDateTime64(120, 3), 2.5)]);

-- Series for the float-only filter: n{job='dup'} carries a histogram, g{job='dup'} is float-only,
-- and f1/f2{job='multi'} are both float-only.
INSERT INTO ts_nh_lot (metric_name, tags, histograms) VALUES
    ('n', map('job', 'dup'), [(toDateTime64(120, 3), 0, -53, 0., 8., 21., 0., [(0, 3)], [0., 2., 6.], [], [], [1., 2., 4.])]);
INSERT INTO ts_nh_lot (metric_name, tags, time_series) VALUES
    ('g', map('job', 'dup'), [(toDateTime64(120, 3), 99)]),
    ('f1', map('job', 'multi'), [(toDateTime64(120, 3), 1)]),
    ('f2', map('job', 'multi'), [(toDateTime64(120, 3), 2)]);

SELECT '-- last_over_time over a subquery, instant, native histogram: the inner grid at -120..120 step 60';
SELECT '-- resolves to [NULL,NULL,NULL,h@60,h@120]; the outer window [-180,120] picks h@120';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_lot', 'last_over_time(nh[300:60])', 120);

SELECT '-- last_over_time over a subquery, instant, float arm: 2.5 at t=120 (this threw before the fix)';
SELECT tags, timestamp, value, histogram FROM prometheusQuery('ts_nh_lot', 'last_over_time(f[300:60])', 120);

SELECT '-- last_over_time over a subquery, range query: every outer step resamples the inner grid,';
SELECT '-- and the inner matrix is NOT leaked on the inner steps (the silent-corruption case before the fix)';
SELECT tags, time_series, histogram_series FROM prometheusQueryRange('ts_nh_lot', 'last_over_time(nh[300:60])', 120, 240, 60);

SELECT '-- histogram_count over a native series plus a float-only series with the same labels: the float-only';
SELECT '-- series is skipped (upstream semantics), so the `__name__` drop sees no duplicate series';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh_lot', 'histogram_count({__name__=~"n|g"})', 120);

SELECT '-- histogram_count over two float-only series: both are skipped, empty result (this threw before the fix)';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh_lot', 'histogram_count({__name__=~"f1|f2"})', 120);

SELECT '-- histogram_quantile over the same colliding selector (its applier had the filter from the start)';
SELECT tags, timestamp, value FROM prometheusQuery('ts_nh_lot', 'histogram_quantile(0.5, {__name__=~"n|g"})', 120);

DROP TABLE ts_nh_lot;
