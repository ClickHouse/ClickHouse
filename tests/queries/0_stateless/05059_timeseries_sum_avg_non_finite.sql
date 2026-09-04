SET allow_experimental_time_series_aggregate_functions = 1;

-- An infinite sample gives an infinite sum and average, not the NaN a compensated sum would produce from `Inf - Inf`.
WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesSumToGrid(110, 110, 50, 50)(timestamps, [inf, 1.]);

WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 50, 50)(timestamps, [inf, 1.]);

-- The same through `merge`: one sample per bucket and 20 buckets per window put the buckets on the two-stacks queue.
WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesSumToGrid(110, 110, 1, 20)(timestamps, [inf, 1.]);

WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 1, 20)(timestamps, [inf, 1.]);

-- Only `Inf + -Inf` and a NaN sample give NaN.
WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesSumToGrid(110, 110, 50, 50)(timestamps, [inf, -inf]);

WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 50, 50)(timestamps, [inf, -inf]);

WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesSumToGrid(110, 110, 1, 20)(timestamps, [inf, -inf]);

WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 1, 20)(timestamps, [inf, -inf]);

WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesSumToGrid(110, 110, 50, 50)(timestamps, [nan, 1.]);

WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 50, 50)(timestamps, [nan, 1.]);

WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesSumToGrid(110, 110, 1, 20)(timestamps, [nan, 1.]);

WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 1, 20)(timestamps, [nan, 1.]);

-- When the sum of finite samples overflows, the summary continues as a running mean like Prometheus does,
-- so the average stays representable while the sum is infinite.
WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 50, 50)(timestamps, [1e308, 1e308]);

WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesSumToGrid(110, 110, 50, 50)(timestamps, [1e308, 1e308]);

-- The same overflow through `merge`.
WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 1, 20)(timestamps, [1e308, 1e308]);

WITH [100, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesSumToGrid(110, 110, 1, 20)(timestamps, [1e308, 1e308]);

-- The running mean also recovers a sum that only overflowed transiently: 1e308 + 1e308 - 1e308 is 1e308.
WITH [100, 105, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesSumToGrid(110, 110, 50, 50)(timestamps, [1e308, 1e308, -1e308]);

WITH [100, 105, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesSumToGrid(110, 110, 1, 20)(timestamps, [1e308, 1e308, -1e308]);

WITH [100, 105, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 50, 50)(timestamps, [1e308, 1e308, -1e308]);

WITH [100, 105, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 1, 20)(timestamps, [1e308, 1e308, -1e308]);

-- With 16 buckets per window the buckets are combined by recomputation in time order, so a summary that has
-- already switched to a mean is merged with a plain sum.
WITH [100, 105, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesSumToGrid(110, 110, 1, 16)(timestamps, [1e308, 1e308, -1e308]);

WITH [100, 105, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 1, 16)(timestamps, [1e308, 1e308, -1e308]);

-- A sum of several samples overflows only at the third one, so a sum of two samples is converted into a mean.
WITH [100, 105, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesSumToGrid(110, 110, 50, 50)(timestamps, [1., 1e308, 1e308]);

WITH [100, 105, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 50, 50)(timestamps, [1., 1e308, 1e308]);

WITH [100, 105, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 1, 20)(timestamps, [1., 1e308, 1e308]);

WITH [100, 105, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 1, 16)(timestamps, [1., 1e308, 1e308]);

-- Infinite samples after the switch to a mean still propagate: `Inf` stays `Inf`, and `Inf + -Inf` gives NaN.
WITH [100, 105, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesSumToGrid(110, 110, 50, 50)(timestamps, [1e308, 1e308, inf]);

WITH [100, 105, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 50, 50)(timestamps, [1e308, 1e308, inf]);

WITH [100, 105, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 1, 20)(timestamps, [1e308, 1e308, inf]);

WITH [100, 105, 110]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(110, 110, 1, 16)(timestamps, [1e308, 1e308, inf]);

WITH [100, 105, 110, 115]::Array(UInt32) AS timestamps
SELECT timeSeriesSumToGrid(115, 115, 50, 50)(timestamps, [1e308, 1e308, inf, -inf]);

WITH [100, 105, 110, 115]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(115, 115, 50, 50)(timestamps, [1e308, 1e308, inf, -inf]);

WITH [100, 105, 110, 115]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(115, 115, 1, 20)(timestamps, [1e308, 1e308, inf, -inf]);

WITH [100, 105, 110, 115]::Array(UInt32) AS timestamps
SELECT timeSeriesAvgToGrid(115, 115, 1, 16)(timestamps, [1e308, 1e308, inf, -inf]);
