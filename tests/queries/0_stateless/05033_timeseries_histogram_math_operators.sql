-- Test: the native-histogram arithmetic scalar functions `timeSeriesHistogram{Add,Sub,MulByScalar,DivByScalar}`
-- and the over-group aggregates `timeSeriesHistogram{Sum,Avg}OverGroup` (the PromQL `sum`/`avg` histogram arms).
--
-- NOTE: this reference file was hand-computed through the upstream algorithm (bit-for-bit verified
-- against a Python recomputation of it; the kernel paths were verified against the pinned upstream
-- Go implementation, see 05031_timeseries_histogram_rate_aggregates).

SET enable_nullable_tuple_type = 1;
SET allow_experimental_time_series_aggregate_functions = 1;

-- The samples below are named like in 05031/05032: e1 = (count 4, sum 10, buckets (0.5,1]x1, (1,2]x3),
-- e2 = (count 8, sum 21, buckets x2, x6), both exponential schema 0.

SELECT '-- add: e1 + e2 = (count 12, sum 31, buckets x3, x9); the counter reset hint stays `unknown` (0)';
SELECT timeSeriesHistogramAdd(e1, e2)
FROM (SELECT (0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS e1,
             (0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS e2);

SELECT '-- sub: e2 - e1 = (count 4, sum 11, buckets x1, x3); the hint is set to gauge (flags 6)';
SELECT timeSeriesHistogramSub(e2, e1)
FROM (SELECT (0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS e1,
             (0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS e2);

SELECT '-- schema mismatch: s1 (schema 1, buckets (0.71,1]x1, (1,1.41]x1, (1.41,2]x6) + e1: s1 is reduced';
SELECT '-- to schema 0 (buckets x1, x7) before adding: (count 12, sum 31, buckets x2, x10)';
SELECT timeSeriesHistogramAdd(s1, e1)
FROM (SELECT (0, 1, 0., 8., 21., 0., [(0, 3)], [1., 1., 6.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS s1,
             (0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS e1);

SELECT '-- custom buckets, same bounds [1,2,4]: c1 + c2 = buckets [1,5,6] over (-Inf,1], (1,2], (2,4]';
SELECT timeSeriesHistogramAdd(c1, c2)
FROM (SELECT (0, -53, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [1., 2., 4.])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS c1,
             (0, -53, 0., 8., 21., 0., [(0, 3)], [0., 2., 6.], [], [], [1., 2., 4.])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS c2);

SELECT '-- custom buckets, mismatched bounds [1,2,4] vs [2,4,8]: the bounds are intersected to [2,4]';
SELECT '-- (upstream nhcbBoundsReconciled); c1 maps to [4,0,0], c3 stays [5,7,0] -> [9,7]';
SELECT timeSeriesHistogramAdd(c1, c3)
FROM (SELECT (0, -53, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [1., 2., 4.])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS c1,
             (0, -53, 0., 4., 14., 0., [(0, 2)], [5., 7.], [], [], [2., 4., 8.])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS c3);

SELECT '-- exp + custom: schema-incompatible -> NULL (upstream drops the sample)';
SELECT timeSeriesHistogramAdd(e1, c1), timeSeriesHistogramSub(e1, c1)
FROM (SELECT (0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS e1,
             (0, -53, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [1., 2., 4.])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS c1);

SELECT '-- zero-threshold reconciliation: zt 0.6 (zero count 1) + zt 0.0: the threshold adjusts up';
SELECT '-- to the bucket bound 1.0 and covered buckets fold into the zero count: zc = 1+1+1 = 3, (1,2] bucket: 3+1 = 4';
SELECT timeSeriesHistogramAdd(z1, z2), timeSeriesHistogramAdd(z2, z1)
FROM (SELECT (0, 0, 0.6, 4., 10., 1., [(0, 2)], [1., 3.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS z1,
             (0, 0, 0.0, 2., 5., 0., [(0, 2)], [1., 1.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS z2);

SELECT '-- mul by scalar 2 / by -2 (a negative factor marks the result as a gauge, flags 6)';
SELECT timeSeriesHistogramMulByScalar(e1, 2), timeSeriesHistogramMulByScalar(e1, -2)
FROM (SELECT (0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS e1);

SELECT '-- div by scalar 2 / by -2 / by 0 (division by zero removes all buckets; the scalar fields';
SELECT '-- still get divided: count/sum +Inf, zero_count 0/0 = NaN)';
SELECT timeSeriesHistogramDivByScalar(e1, 2), timeSeriesHistogramDivByScalar(e1, -2), timeSeriesHistogramDivByScalar(e1, 0)
FROM (SELECT (0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS e1);

SELECT '-- NULL handling: NULL in -> NULL out for all four';
SELECT timeSeriesHistogramAdd(e1, h), timeSeriesHistogramSub(e1, h), timeSeriesHistogramMulByScalar(h, 2), timeSeriesHistogramDivByScalar(h, 2)
FROM (SELECT (0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS e1,
             CAST(NULL AS Nullable(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)))) AS h);

SELECT '-- row-wise NULLs inside Nullable columns are kept per row (the first row is e1 + e1)';
SELECT timeSeriesHistogramAdd(a, b)
FROM (SELECT CAST([(0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], []), NULL], 'Array(Nullable(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64))))') AS arr)
ARRAY JOIN arr AS a, arr AS b;

SELECT '-- the counter reset hint of `sum`: two NotCounterReset samples (flags 4) keep the hint (flags 4)';
SELECT timeSeriesHistogramAdd(a, b)
FROM (SELECT (4, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS a,
             (4, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS b);

SELECT '-- aggregate level: `timeSeriesHistogramSumOverGroup` / `timeSeriesHistogramAvgOverGroup` over e1, e2';
SELECT '-- sum = (count 12, sum 31, buckets x3, x9); avg = sum/2 = (count 6, sum 15.5, buckets x1.5, x4.5)';
SELECT timeSeriesHistogramSumOverGroup(h), timeSeriesHistogramAvgOverGroup(h)
FROM (SELECT arrayJoin([(0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], []), (0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], [])]::Array(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)))) AS h);

SELECT '-- a single-sample group is the sample itself';
SELECT timeSeriesHistogramSumOverGroup(h), timeSeriesHistogramAvgOverGroup(h)
FROM (SELECT arrayJoin([(0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])]::Array(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)))) AS h);

SELECT '-- custom-bucket group: sum/avg over c1, c2';
SELECT timeSeriesHistogramSumOverGroup(h), timeSeriesHistogramAvgOverGroup(h)
FROM (SELECT arrayJoin([(0, -53, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [1., 2., 4.]), (0, -53, 0., 8., 21., 0., [(0, 3)], [0., 2., 6.], [], [], [1., 2., 4.])]::Array(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)))) AS h);

SELECT '-- schema-incompatible group (exp + custom): NULL for both (upstream drops the group element)';
SELECT timeSeriesHistogramSumOverGroup(h), timeSeriesHistogramAvgOverGroup(h)
FROM (SELECT arrayJoin([(0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], []), (0, -53, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [1., 2., 4.])]::Array(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)))) AS h);

SELECT '-- an empty group produces NULL';
SELECT timeSeriesHistogramSumOverGroup(h), timeSeriesHistogramAvgOverGroup(h)
FROM (SELECT arrayJoin([]::Array(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)))) AS h);

SELECT '-- Kahan accumulation: big (count 1, sum 1e16) + two smalls (count 1, sum 1): the naive sum loses';
SELECT '-- the ones (ulp(1e16) = 2); Kahan keeps them: sum = 1e16+2, avg = 3333333333333334 (naive ...3333.5)';
SELECT timeSeriesHistogramSumOverGroup(h), timeSeriesHistogramAvgOverGroup(h)
FROM (SELECT arrayJoin([(0, 0, 0., 1., 1e16, 0., [(0, 1)], [1e16], [], [], []), (0, 0, 0., 1., 1., 0., [(0, 1)], [1.], [], [], []), (0, 0, 0., 1., 1., 0., [(0, 1)], [1.], [], [], [])]::Array(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)))) AS h);

SELECT '-- overflow: two samples with count = sum = bucket = 1.5e308; the `sum` overflows to +Inf';
SELECT '-- (upstream keeps it), while `avg` switches to the incremental mean and stays finite (1.5e308)';
SELECT timeSeriesHistogramSumOverGroup(h), timeSeriesHistogramAvgOverGroup(h)
FROM (SELECT arrayJoin([(0, 0, 0., 1.5e308, 1.5e308, 0., [(0, 1)], [1.5e308], [], [], []), (0, 0, 0., 1.5e308, 1.5e308, 0., [(0, 1)], [1.5e308], [], [], [])]::Array(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)))) AS h);

SELECT '-- the -ForEach form over a two-step grid: position 1 aggregates {e1, e2}, position 2 {e2}';
SELECT '-- (the NULL of the first series is skipped)';
SELECT timeSeriesHistogramSumOverGroupForEach(arr), timeSeriesHistogramAvgOverGroupForEach(arr)
FROM (SELECT arrayJoin([
        [(0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], []), NULL]::Array(Nullable(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)))),
        [(0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], []), (0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], [])]::Array(Nullable(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64))))]) AS arr);
