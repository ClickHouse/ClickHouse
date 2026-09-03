-- Test: `timeSeriesHistogramAvg`, `timeSeriesHistogramStddev` and `timeSeriesHistogramStdvar` over
-- hand-built native-histogram payload tuples (see `getTimeSeriesHistogramPayloadTupleType`),
-- mirroring PromQL `histogram_avg` / `histogram_stddev` / `histogram_stdvar` (`histogramVariance`
-- over `AllBucketIterator` in Prometheus promql/functions.go: bucket population x representative
-- value, squared deviations from `sum`/`count`, compensated summation).
--
-- NOTE: this reference file was verified bit-for-bit against a standalone C++ run of the exact
-- operation sequence and a Python recomputation of the upstream algorithm.

SET enable_nullable_tuple_type = 1;
SELECT '-- schema 0, positive buckets [1,2]x3 and [2,4]x1, count 4, sum 4*sqrt(2) so mean = sqrt(2)';
-- Math: bucket representative values are the geometric means sqrt(1*2) = sqrt(2) and
-- sqrt(2*4) = 2*sqrt(2); variance = (3*(sqrt(2)-sqrt(2))^2 + 1*(2*sqrt(2)-sqrt(2))^2)/4 = 2/4 = 0.5
-- (one ulp is lost in float64: 0.5000000000000001), stddev = sqrt(variance), avg = sqrt(2).
SELECT timeSeriesHistogramAvg(h), timeSeriesHistogramStddev(h), timeSeriesHistogramStdvar(h)
FROM (SELECT (0, 0, 0., 4., 5.656854249492381, 0., [(1, 2)], [3., 1.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- count = 0: 0/0 -> NaN for all three (upstream behavior, no special-casing)';
SELECT timeSeriesHistogramAvg(h), timeSeriesHistogramStddev(h), timeSeriesHistogramStdvar(h)
FROM (SELECT (0, 0, 0., 0., 0., 0., [], [], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- NULL in -> NULL out (default null handling)';
SELECT timeSeriesHistogramAvg(h), timeSeriesHistogramStddev(h), timeSeriesHistogramStdvar(h)
FROM (SELECT CAST(NULL AS Nullable(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)))) AS h);

SELECT '-- row-wise NULLs inside a Nullable column are kept per row (second row: the doc-example histogram)';
-- Math for the second row: custom buckets [1,3,5], buckets [1,3)x3 (value (1+3)/2 = 2) and
-- [3,5)x1 (value (3+5)/2 = 4), count 4, sum 8 -> mean 2, variance = (3*0^2 + 1*2^2)/4 = 1, stddev = 1.
SELECT timeSeriesHistogramAvg(h), timeSeriesHistogramStddev(h), timeSeriesHistogramStdvar(h)
FROM (SELECT CAST([NULL, (0, -53, 0., 4., 8., 0., [(1, 2)], [3., 1.], [], [], [1., 3., 5.])], 'Array(Nullable(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64))))') AS arr)
ARRAY JOIN arr AS h;

SELECT '-- custom buckets (schema -53), bounds [1,2,4]; the [-Inf,1) bucket has count 0 and is skipped';
-- Math: buckets [1,2)x2 (value 1.5) and [2,4)x6 (value 3); count 8, sum 21 -> mean = 21/8 = 2.625;
-- variance = (2*(1.5-2.625)^2 + 6*(3-2.625)^2)/8 = (2*1.265625 + 6*0.140625)/8 = 3.375/8 = 0.421875
-- (exact in float64), stddev = sqrt(0.421875).
SELECT timeSeriesHistogramAvg(h), timeSeriesHistogramStddev(h), timeSeriesHistogramStdvar(h)
FROM (SELECT (0, -53, 0., 8., 21., 0., [(0, 3)], [0., 2., 6.], [], [], [1., 2., 4.])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- negative-only buckets [-2,-1]x3 and [-4,-2]x1: mirror of the first case, mean = -sqrt(2)';
-- Math: bucket values are negated geometric means: -sqrt(2) (count 3) and -2*sqrt(2) (count 1);
-- variance = (1*(-2*sqrt(2)+sqrt(2))^2 + 3*0^2)/4 = 0.5 (0.5000000000000001 in float64), avg = -sqrt(2).
SELECT timeSeriesHistogramAvg(h), timeSeriesHistogramStddev(h), timeSeriesHistogramStdvar(h)
FROM (SELECT (0, 0, 0., 4., -5.656854249492381, 0., [], [], [(1, 2)], [3., 1.], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- zero bucket + zero-threshold clamping of the nearest exponential buckets, zero_threshold 0.6';
-- Math: negative bucket [-1,-0.5] clamps its upper bound to -0.6 -> [-1,-0.6] (value -sqrt(0.6*1),
-- count 1); the zero bucket [-0.6,0.6] has value 0 (count 1); positive bucket [0.5,1] clamps its
-- lower bound to 0.6 -> [0.6,1] (value sqrt(0.6*1), count 1). count 3, sum 0 -> mean 0;
-- variance = (0.6 + 0 + 0.6)/3 = 0.4 (0.4000000000000001 in float64), avg = 0.
SELECT timeSeriesHistogramAvg(h), timeSeriesHistogramStddev(h), timeSeriesHistogramStdvar(h)
FROM (SELECT (0, 0, 0.6, 3., 0., 1., [(0, 1)], [1.], [(0, 1)], [1.], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- an invalid exponential schema (9 > 8) is rejected';
SELECT timeSeriesHistogramStdvar((0, 9, 0., 4., 5.656854249492381, 0., [(1, 2)], [3., 1.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64))); -- { serverError INCORRECT_DATA }
