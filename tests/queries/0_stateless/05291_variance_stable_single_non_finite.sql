-- A single non-finite value: the population variance and the standard deviation are NaN, not 0.
SELECT 'single nan', varPopStable(x), stddevPopStable(x) FROM (SELECT arrayJoin([nan]) AS x);
SELECT 'single inf', varPopStable(x), stddevPopStable(x) FROM (SELECT arrayJoin([inf]) AS x);
SELECT 'single -inf', varPopStable(x), stddevPopStable(x) FROM (SELECT arrayJoin([-inf]) AS x);

-- A single finite value, including one whose square overflows Float64, has a variance of 0.
SELECT 'single 1', varPopStable(x), stddevPopStable(x) FROM (SELECT arrayJoin([1.]) AS x);
SELECT 'single 1e200', varPopStable(x), stddevPopStable(x) FROM (SELECT arrayJoin([1e200]) AS x);
SELECT 'single -1e200', varPopStable(x), stddevPopStable(x) FROM (SELECT arrayJoin([-1e200]) AS x);

-- Non-finite values among several rows stay NaN.
SELECT 'nan nan', varPopStable(x), stddevPopStable(x) FROM (SELECT arrayJoin([nan, nan]) AS x);
SELECT '1 2 nan', varPopStable(x), stddevPopStable(x) FROM (SELECT arrayJoin([1., 2., nan]) AS x);
SELECT '1 2 inf', varPopStable(x), stddevPopStable(x) FROM (SELECT arrayJoin([1., 2., inf]) AS x);

-- The sample variance of a single value stays +Inf.
SELECT 'samp single 1', varSampStable(x), stddevSampStable(x) FROM (SELECT arrayJoin([1.]) AS x);
SELECT 'samp single nan', varSampStable(x), stddevSampStable(x) FROM (SELECT arrayJoin([nan]) AS x);

-- A merge with an empty state does not change the result, even when the squared mean overflows Float64.
SELECT 'merge empty + 1e200', varPopStableMerge(s) FROM (SELECT varPopStableState(x) AS s FROM (SELECT arrayJoin([]::Array(Float64)) AS x) UNION ALL SELECT varPopStableState(x) FROM (SELECT arrayJoin([1e200]) AS x));
SELECT 'merge 1e200 1e200 + empty', varPopStableMerge(s) FROM (SELECT varPopStableState(x) AS s FROM (SELECT arrayJoin([1e200, 1e200]) AS x) UNION ALL SELECT varPopStableState(x) FROM (SELECT arrayJoin([]::Array(Float64)) AS x));
SELECT 'merge empty + 1e200 1e200', varPopStableMerge(s) FROM (SELECT varPopStableState(x) AS s FROM (SELECT arrayJoin([]::Array(Float64)) AS x) UNION ALL SELECT varPopStableState(x) FROM (SELECT arrayJoin([1e200, 1e200]) AS x));
SELECT 'merge nan + empty', varPopStableMerge(s) FROM (SELECT varPopStableState(x) AS s FROM (SELECT arrayJoin([nan]) AS x) UNION ALL SELECT varPopStableState(x) FROM (SELECT arrayJoin([]::Array(Float64)) AS x));
SELECT 'merge empty + empty', varPopStableMerge(s) FROM (SELECT varPopStableState(x) AS s FROM (SELECT arrayJoin([]::Array(Float64)) AS x) UNION ALL SELECT varPopStableState(x) FROM (SELECT arrayJoin([]::Array(Float64)) AS x));
SELECT 'merge 1 + 3', varPopStableMerge(s) FROM (SELECT varPopStableState(x) AS s FROM (SELECT arrayJoin([1.]) AS x) UNION ALL SELECT varPopStableState(x) FROM (SELECT arrayJoin([3.]) AS x));

-- The -ForEach combinator over Nullable arrays, as PromQL stddev and stdvar use it: one position per series group.
SELECT 'foreach', stddevPopStableForEach(v), varPopStableForEach(v) FROM (SELECT [toNullable(1.), toNullable(nan), toNullable(inf), NULL::Nullable(Float64)] AS v);
SELECT 'foreach two rows', stddevPopStableForEach(v), varPopStableForEach(v) FROM (SELECT [toNullable(1.), toNullable(2.), toNullable(1.)] AS v UNION ALL SELECT [toNullable(3.), toNullable(nan), NULL::Nullable(Float64)]);
