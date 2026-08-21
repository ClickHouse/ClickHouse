-- Test: `timeSeriesHistogramQuantile` and `timeSeriesHistogramFraction` over hand-built
-- native-histogram payload tuples (see `getTimeSeriesHistogramPayloadTupleType`), mirroring
-- `HistogramQuantile` / `HistogramFraction` in Prometheus promql/quantile.go (bucket walk of
-- `AllBucketIterator`/`AllReverseBucketIterator` in model/histogram/float_histogram.go:
-- negative buckets from the most negative one up, the zero bucket, the positive buckets, with
-- zero-threshold clamping; linear interpolation for custom buckets and the zero bucket,
-- log-scale interpolation for exponential buckets).
--
-- NOTE: this reference file was verified bit-for-bit against a standalone C++ run of the exact
-- operation sequence plus a Python recomputation (bit-identical), and cross-checked against the
-- upstream Go implementation run on the real prometheus/model/histogram package.
-- Residual ulp risk: upstream's math.Log2/math.Exp2 deviate by 1 ulp from a correctly-rounded
-- libm on some inputs (cases are marked below); ClickHouse uses the platform libm, which is
-- correctly rounded on the CI platforms.

SET enable_nullable_tuple_type = 1;
SELECT '-- exponential schema 0, buckets [1,2]x2 and [2,4]x2, count 4';
SELECT '-- q=0.5: reverse walk, rank (1-0.5)*4 = 2 lands on [2,4] with fraction 0 -> exp2(log2(2)) = 2 exactly';
SELECT timeSeriesHistogramQuantile(h, 0.5) FROM (SELECT (0, 0, 0., 4., 6., 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- q=0.25: forward walk, rank 1 lands on [1,2] with fraction 1/2 -> exp2(0 + (1-0)*1/2) = sqrt(2)';
SELECT '-- (upstream Go prints 0x1.6a09e667f3bccp+0 here: its math.Exp2(0.5) is 1 ulp below the correctly-rounded sqrt(2))';
SELECT timeSeriesHistogramQuantile(h, 0.25) FROM (SELECT (0, 0, 0., 4., 6., 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- q=0.5 on buckets [1,2]x3 and [2,4]x1: reverse walk ends in [1,2], fraction 2/3 -> exp2(2/3)';
SELECT timeSeriesHistogramQuantile(h, 0.5) FROM (SELECT (0, 0, 0., 4., 5.656854249492381, 0., [(1, 2)], [3., 1.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- q=0 and q=1: the extreme fractions 0 and 1 of the first/last bucket -> 1 and 4 exactly';
SELECT timeSeriesHistogramQuantile(h, 0), timeSeriesHistogramQuantile(h, 1) FROM (SELECT (0, 0, 0., 4., 6., 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- negative buckets [-2,-1]x1 and [-4,-2]x3, count 4';
SELECT '-- q=0.75: reverse walk, rank (1-0.75)*4 = 1 lands on [-2,-1] with fraction 0 -> -exp2(log2(2)) = -2 exactly';
SELECT '-- q=0.5: reverse walk ends in [-4,-2], fraction 2/3 -> -exp2(1 + (2-1)*1/3) = -exp2(4/3)';
SELECT '-- (upstream Go prints -0x1.428a2f98d728cp+1 for the second: its math.Exp2(4/3) is 1 ulp higher)';
SELECT timeSeriesHistogramQuantile(h, 0.75), timeSeriesHistogramQuantile(h, 0.5) FROM (SELECT (0, 0, 0., 4., -7., 0., [], [], [(1, 2)], [1., 3.], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- zero bucket, positive buckets only: the natural lower bound 0 is assumed';
SELECT '-- zt 0.5, zero bucket [-0.5,0.5]x2, [1,2]x2, count 4; q=0.25: rank 1 in the zero bucket -> [0,0.5], fraction 1/2 -> 0.25 exactly';
SELECT timeSeriesHistogramQuantile(h, 0.25) FROM (SELECT (0, 0, 0.5, 4., 2., 2., [(1, 1)], [2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- zero bucket, negative buckets only: the natural upper bound 0 is assumed';
SELECT '-- zt 0.5, [-1,-0.5]x2, zero bucket x2, count 4; q=0.75: reverse rank 1 in the zero bucket -> [-0.5,0], fraction 1/2 -> -0.25 exactly';
SELECT timeSeriesHistogramQuantile(h, 0.75) FROM (SELECT (0, 0, 0.5, 4., -2., 2., [], [], [(0, 1)], [2.], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- zero bucket with BOTH sides present: no natural-bound adjustment, linear interpolation over [-0.6,0.6]';
SELECT '-- zt 0.6, [-1,-0.5]x1 (clamped to [-1,-0.6]), zero x1, [0.5,1]x1 (clamped to [0.6,1]), count 3; q=0.5: reverse rank 1.5 in the zero bucket, fraction 0.5 -> -0.6 + 1.2*0.5 = 0 exactly';
SELECT timeSeriesHistogramQuantile(h, 0.5) FROM (SELECT (0, 0, 0.6, 3., 0., 1., [(0, 1)], [1.], [(0, 1)], [1.], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- zero-threshold clamping of the quantile bucket itself: zt 0.6, buckets [0.5,1]x2 -> [0.6,1]x2 and [1,2]x2';
SELECT '-- q=0.25: rank 1 in [0.6,1], fraction 1/2 -> exp2(log2(0.6)/2) = sqrt(0.6)';
SELECT timeSeriesHistogramQuantile(h, 0.25) FROM (SELECT (0, 0, 0.6, 4., 6., 0., [(0, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- schema 1, buckets (0.7071,1]x2 and (1,1.4142]x2 (the schema-1 table entries 0.5 and 0.7071067811865475), count 4';
SELECT '-- q=0.5: reverse rank 2 lands on (1,1.4142] with fraction 0 -> 1 exactly';
SELECT '-- q=0.25: forward rank 1 lands on (0.7071,1] with fraction 1/2 -> exp2(log2(0.7071067811865475)*1/2)';
SELECT timeSeriesHistogramQuantile(h, 0.5), timeSeriesHistogramQuantile(h, 0.25) FROM (SELECT (0, 1, 0., 4., 3., 0., [(0, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- custom buckets (schema -53), bounds [1,2,4], buckets [-Inf,1]x0, [1,2]x2, [2,4]x6, count 8';
SELECT '-- q=0.5: reverse rank 4 lands on [2,4], fraction 2/6 = 1/3, linear -> 2 + 2*1/3';
SELECT timeSeriesHistogramQuantile(h, 0.5) FROM (SELECT (0, -53, 0., 8., 21., 0., [(0, 3)], [0., 2., 6.], [], [], [1., 2., 4.])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- custom buckets [-2,-1], buckets [-Inf,-2]x3 and [-2,-1]x1, count 4';
SELECT '-- q=0.5: reverse rank 2 lands on the -Inf bucket whose upper bound <= 0 -> the upper bound -2 exactly';
SELECT timeSeriesHistogramQuantile(h, 0.5) FROM (SELECT (0, -53, 0., 4., -5., 0., [(0, 2)], [3., 1.], [], [], [-2., -1.])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- custom buckets [1,2,4], buckets [-Inf,1]x0, [1,2]x0, [2,4]x3, [4,+Inf]x2, count 5';
SELECT '-- q=0.9: reverse rank 0.5 lands on the +Inf bucket -> its lower bound 4 exactly';
SELECT timeSeriesHistogramQuantile(h, 0.9) FROM (SELECT (0, -53, 0., 5., 15., 0., [(0, 4)], [0., 0., 3., 2.], [], [], [1., 2., 4.])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- out-of-range phi: q < 0 -> -Inf, q > 1 -> +Inf, q NaN -> NaN (constant per the PromQL semantics)';
SELECT timeSeriesHistogramQuantile(h, -0.5), timeSeriesHistogramQuantile(h, 1.5), timeSeriesHistogramQuantile(h, nan) FROM (SELECT (0, 0, 0., 4., 6., 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- count = 0 -> NaN (upstream behavior, no special-casing)';
SELECT timeSeriesHistogramQuantile(h, 0.5), timeSeriesHistogramFraction(h, 0, 1) FROM (SELECT (0, 0, 0., 0., 0., 0., [], [], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- NaN observations (sum is NaN, count 5 > the 4 bucketed observations): q=0.9 forces the forward walk,';
SELECT '-- the accumulated count 4 never reaches the rank 4.5 -> NaN (upstream issue 16578)';
SELECT timeSeriesHistogramQuantile(h, 0.9) FROM (SELECT (0, 0, 0., 5., nan, 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- NULL in -> NULL out (default null handling)';
SELECT timeSeriesHistogramQuantile(h, 0.5), timeSeriesHistogramFraction(h, 0, 1)
FROM (SELECT CAST(NULL AS Nullable(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)))) AS h);

SELECT '-- row-wise NULLs inside a Nullable column are kept per row (second row: the q9 custom-bucket histogram)';
SELECT timeSeriesHistogramQuantile(h, 0.5), timeSeriesHistogramFraction(h, 1.5, 3)
FROM (SELECT CAST([NULL, (0, -53, 0., 8., 21., 0., [(0, 3)], [0., 2., 6.], [], [], [1., 2., 4.])], 'Array(Nullable(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64))))') AS arr)
ARRAY JOIN arr AS h;

SELECT '-- an invalid exponential schema (9 > 8) is rejected';
SELECT timeSeriesHistogramQuantile((0, 9, 0., 4., 6., 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)), 0.5); -- { serverError INCORRECT_DATA }

SELECT '-- a non-constant phi is rejected (the PromQL converter enforces a constant phi; this is defense in depth)';
SELECT timeSeriesHistogramQuantile(h, phi) FROM (SELECT (0, 0, 0., 4., 6., 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h, arrayJoin([0.5, 0.9]) AS phi); -- { serverError ILLEGAL_COLUMN }

SELECT '-- a non-numeric phi is rejected';
SELECT timeSeriesHistogramQuantile((0, 0, 0., 4., 6., 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)), 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- fraction: exponential schema 0, buckets [1,2]x2 and [2,4]x2, count 4';
SELECT '-- (1, 2): both bounds hit bucket lower boundaries -> lowerRank 0, upperRank 2 -> 0.5; (-Inf, +Inf) -> 1';
SELECT timeSeriesHistogramFraction(h, 1, 2), timeSeriesHistogramFraction(h, -inf, inf) FROM (SELECT (0, 0, 0., 4., 6., 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- fraction (1, 3): the doc example; upperRank = 2 + 2*(log2(3)-1), so (2 + 2*(log2(3)-1))/4';
SELECT '-- (upstream Go prints 0x1.95c01a39fbd69p-1 here: its math.Log2(3) is 1 ulp above the correctly-rounded value)';
SELECT timeSeriesHistogramFraction(h, 1, 3) FROM (SELECT (0, 0, 0., 4., 6., 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- fraction (1.5, 3): log-scale interpolation in both buckets: lowerRank = 2*log2(1.5), upperRank = 2 + 2*(log2(3)-1)';
SELECT '-- (with a correctly-rounded log2, log2(3)-1 is 1 ulp below log2(1.5), so the ranks do not cancel to 2;';
SELECT '--  upstream Go whose math.Log2(3) is 1 ulp high returns exactly 0.5 here)';
SELECT timeSeriesHistogramFraction(h, 1.5, 3) FROM (SELECT (0, 0, 0., 4., 6., 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- fraction with lower >= upper -> 0; NaN bounds -> NaN';
SELECT timeSeriesHistogramFraction(h, 2.5, 0.5), timeSeriesHistogramFraction(h, nan, 1), timeSeriesHistogramFraction(h, 0, nan) FROM (SELECT (0, 0, 0., 4., 6., 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- fraction (5, 100): both bounds above all buckets -> 0';
SELECT timeSeriesHistogramFraction(h, 5, 100) FROM (SELECT (0, 0, 0., 4., 6., 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- fraction with NaN observations (sum NaN, count 5 > 4 bucketed): NaN observations are in no bucket,';
SELECT '-- so (-Inf, +Inf) yields the walked count 4 over count 5 -> 0.8';
SELECT timeSeriesHistogramFraction(h, -inf, inf) FROM (SELECT (0, 0, 0., 5., nan, 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- fraction over the zero bucket (linear): zt 0.5, zero bucket x2, [1,2]x2, count 4';
SELECT '-- (0.25, 1.5): lowerRank = 2*(0.25/0.5) = 1 in the zero bucket [0,0.5]; upperRank = 2 + 2*log2(1.5) in [1,2]';
SELECT '-- -> (2 + 2*log2(1.5) - 1)/4 (upstream Go agrees bit-for-bit)';
SELECT timeSeriesHistogramFraction(h, 0.25, 1.5) FROM (SELECT (0, 0, 0.5, 4., 2., 2., [(1, 1)], [2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- fraction over a negative-only zero-bucket histogram: zt 0.5, [-1,-0.5]x2, zero x2, count 4';
SELECT '-- (-0.25, 0.25): the zero bucket becomes [-0.5,0]; lowerRank = 2 + 2*(0.25/0.5) = 3; upperRank -> count -> (4-3)/4 = 0.25';
SELECT timeSeriesHistogramFraction(h, -0.25, 0.25) FROM (SELECT (0, 0, 0.5, 4., -2., 2., [], [], [(0, 1)], [2.], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- fraction over negative exponential buckets [-2,-1]x2, [-4,-2]x2, count 4';
SELECT '-- (-3, -1): lowerRank = 2*(1 - (log2(3)-1)/(log2(4)-log2(2))) in [-4,-2]; upperRank -> count';
SELECT '-- (upstream Go prints 0x1.95c01a39fbd69p-1 here: its math.Log2(3) is 1 ulp above the correctly-rounded value)';
SELECT timeSeriesHistogramFraction(h, -3, -1) FROM (SELECT (0, 0, 0., 4., -6., 0., [], [], [(1, 2)], [2., 2.], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- fraction over custom buckets [1,2,4] with counts [0,2,6], count 8';
SELECT '-- (1.5, 3): linear in [1,2] -> lowerRank 2*0.5 = 1; linear in [2,4] -> upperRank 2 + 6*0.5 = 5 -> 0.5';
SELECT timeSeriesHistogramFraction(h, 1.5, 3) FROM (SELECT (0, -53, 0., 8., 21., 0., [(0, 3)], [0., 2., 6.], [], [], [1., 2., 4.])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- fraction over custom buckets [1,2] with counts [4,4], count 8';
SELECT '-- (0, 1.5): the [-Inf,1] bucket spans zero, so the zero-bucket adjustment makes it [0,1] and lowerRank = 0';
SELECT '-- (mirroring upstream, which applies the adjustment to custom buckets too); upperRank = 4 + 4*0.5 = 6 -> 0.75';
SELECT timeSeriesHistogramFraction(h, 0, 1.5) FROM (SELECT (0, -53, 0., 8., 12., 0., [(0, 2)], [4., 4.], [], [], [1., 2.])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);

SELECT '-- fraction over custom buckets [-2,-1] with counts [3,1], count 4';
SELECT '-- (-3, -1): the -Inf bucket contributes its whole count 3 as lowerRank (upstream skips interpolation there)';
SELECT '-- (-1.5, 1): lowerRank = 3 + 1*0.5 = 3.5, upperRank -> count -> (4-3.5)/4 = 0.125';
SELECT timeSeriesHistogramFraction(h, -3, -1), timeSeriesHistogramFraction(h, -1.5, 1) FROM (SELECT (0, -53, 0., 4., -5., 0., [(0, 2)], [3., 1.], [], [], [-2., -1.])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h);
