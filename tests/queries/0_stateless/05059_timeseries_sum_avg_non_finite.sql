SET allow_experimental_time_series_aggregate_functions = 1;

-- An infinite sample or an overflowing sum gives an infinite sum and average instead of the NaN a compensated sum
-- would produce from `Inf - Inf`. Only `Inf + -Inf` and a NaN sample give NaN.
-- For `[1e308, 1e308]` the average 1e308 is representable, and Prometheus returns it by switching to an incremental
-- mean when the sum overflows; this implementation averages per-bucket sums, so it returns Inf there.
-- The single-point grid folds both samples in one bucket; the dense grid has 50 buckets per window, so the two
-- buckets are merged through the two-stacks queue.
WITH arrayMap(x -> toDateTime64(x, 3, 'UTC'), [100, 110]) AS ts
SELECT
    vals,
    timeSeriesSumToGrid(110, 110, 1, 50)(ts, vals)[1] AS sum_single_bucket,
    timeSeriesAvgToGrid(110, 110, 1, 50)(ts, vals)[1] AS avg_single_bucket,
    timeSeriesSumToGrid(100, 140, 1, 50)(ts, vals)[41] AS sum_two_stacks,
    timeSeriesAvgToGrid(100, 140, 1, 50)(ts, vals)[41] AS avg_two_stacks
FROM (SELECT arrayJoin([[inf, 1.], [1e308, 1e308], [inf, -inf], [nan, 1.]]) AS vals)
GROUP BY vals
ORDER BY vals;
