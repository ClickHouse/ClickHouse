-- Merging states of the stable variance and covariance functions gave NaN for large values: merging with an empty state,
-- `delta * delta` overflowed to infinity and was multiplied by a zero factor. Every merge starts from an empty state, so
-- `-Merge` and parallel aggregation were affected. Two values are used in each case: for one value the population
-- variance is 0 whatever the state holds.

SELECT '--- merge of two states of equal large values ---';
SELECT stddevPopStableMerge(s) FROM (SELECT arrayJoin([stddevPopStableState(x), stddevPopStableState(x)]) AS s FROM (SELECT 1e155 AS x));
SELECT varPopStableMerge(s) FROM (SELECT arrayJoin([varPopStableState(x), varPopStableState(x)]) AS s FROM (SELECT 1e155 AS x));
SELECT stddevSampStableMerge(s) FROM (SELECT arrayJoin([stddevSampStableState(x), stddevSampStableState(x)]) AS s FROM (SELECT 1e155 AS x));
SELECT varSampStableMerge(s) FROM (SELECT arrayJoin([varSampStableState(x), varSampStableState(x)]) AS s FROM (SELECT 1e155 AS x));
SELECT covarPopStableMerge(s) FROM (SELECT arrayJoin([covarPopStableState(x, x), covarPopStableState(x, x)]) AS s FROM (SELECT 1e155 AS x));
SELECT covarSampStableMerge(s) FROM (SELECT arrayJoin([covarSampStableState(x, x), covarSampStableState(x, x)]) AS s FROM (SELECT 1e155 AS x));

SELECT '--- an empty state in the middle ---';
SELECT finalizeAggregation(stddevPopStableState(x) + stddevPopStableStateIf(x, x < 0) + stddevPopStableState(x)) FROM (SELECT 1e155 AS x);
SELECT finalizeAggregation(covarPopStableState(x, x) + covarPopStableStateIf(x, x, x < 0) + covarPopStableState(x, x)) FROM (SELECT 1e155 AS x);

SELECT '--- parallel aggregation ---';
SELECT stddevPopStable(x) FROM (SELECT 1e155 AS x FROM numbers_mt(8)) SETTINGS max_threads = 8, max_block_size = 1;

SELECT '--- small values are unchanged ---';
SELECT varPopStableMerge(s) FROM (SELECT arrayJoin([varPopStableState(x), varPopStableState(x + 1)]) AS s FROM (SELECT 1. AS x));
SELECT round(corrStableMerge(s), 6) FROM (SELECT arrayJoin([corrStableState(x, 2 * x), corrStableState(x + 1, 2 * x + 2)]) AS s FROM (SELECT 1. AS x));
