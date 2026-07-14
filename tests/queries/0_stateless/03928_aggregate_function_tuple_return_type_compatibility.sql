-- { echo }

-- Compatibility coverage check for tuple-returning aggregate functions with Nullable arguments.
-- sumCount is covered separately in 03927_sumcount_compatibility.
-- Make sure serialized states of these functions can be deserialized.

-- simpleLinearRegression non-empty state.
SELECT finalizeAggregation(CAST(unhex('01040000000000000000000000000028400000000000003C4000000000000047400000000000005A40'), 'AggregateFunction(simpleLinearRegression, Nullable(Float64), Nullable(Float64))'));

-- analysisOfVariance non-empty state.
SELECT tuple(roundBankers(res.1, 4), roundBankers(res.2, 4))
FROM
(
    SELECT finalizeAggregation(CAST(unhex('0102000000000000224000000000000028400200000000008041400000000000005A400203000000000000000200000000000000'), 'AggregateFunction(analysisOfVariance, Nullable(Float64), Nullable(UInt8))')) AS res
);

-- kolmogorovSmirnovTest non-empty state.
SELECT tuple(roundBankers(res.1, 4), roundBankers(res.2, 4))
FROM
(
    SELECT finalizeAggregation(CAST(unhex('010302000000000000F03F0000000000000840000000000000144000000000000000400000000000002440'), 'AggregateFunction(kolmogorovSmirnovTest(''two-sided''), Nullable(Float64), Nullable(UInt8))')) AS res
);

-- mannWhitneyUTest non-empty state.
SELECT tuple(roundBankers(res.1, 4), roundBankers(res.2, 4))
FROM
(
    SELECT finalizeAggregation(CAST(unhex('010302000000000000F03F0000000000000840000000000000144000000000000000400000000000002440'), 'AggregateFunction(mannWhitneyUTest(''two-sided''), Nullable(Float64), Nullable(UInt8))')) AS res
);

-- studentTTest non-empty state.
SELECT tuple(roundBankers(res.1, 4), roundBankers(res.2, 4))
FROM
(
    SELECT finalizeAggregation(CAST(unhex('01000000000000084000000000000000400000000000002240000000000000284000000000008041400000000000005A40'), 'AggregateFunction(studentTTest, Nullable(Float64), Nullable(UInt8))')) AS res
);

-- welchTTest non-empty state.
SELECT tuple(roundBankers(res.1, 4), roundBankers(res.2, 4))
FROM
(
    SELECT finalizeAggregation(CAST(unhex('01000000000000084000000000000000400000000000002240000000000000284000000000008041400000000000005A40'), 'AggregateFunction(welchTTest, Nullable(Float64), Nullable(UInt8))')) AS res
);

-- meanZTest non-empty state.
SELECT tuple(roundBankers(res.1, 4), roundBankers(res.2, 4), roundBankers(res.3, 4), roundBankers(res.4, 4))
FROM
(
    SELECT finalizeAggregation(CAST(unhex('010000000000000840000000000000004000000000000022400000000000002840'), 'AggregateFunction(meanZTest(1., 1., 0.95), Nullable(Float64), Nullable(UInt8))')) AS res
);

-- studentTTestOneSample non-empty state.
SELECT tuple(roundBankers(res.1, 4), roundBankers(res.2, 4))
FROM
(
    SELECT finalizeAggregation(CAST(unhex('010000000000000840000000000000224000000000008041400000000000000040'), 'AggregateFunction(studentTTestOneSample, Nullable(Float64), Nullable(Float64))')) AS res
);

-- argAndMin non-empty state.
SELECT finalizeAggregation(CAST(unhex('0101030000000100000000'), 'AggregateFunction(argAndMin, Nullable(Int32), Nullable(Int32))'));

-- argAndMax non-empty state.
SELECT finalizeAggregation(CAST(unhex('0101010000000102000000'), 'AggregateFunction(argAndMax, Nullable(Int32), Nullable(Int32))'));

-- argMin(tuple, val) non-empty state.
SELECT finalizeAggregation(CAST(unhex('01010100010000000100000000'), 'AggregateFunction(argMin, Tuple(Nullable(Int32), Nullable(Int32)), Nullable(Int32))'));

-- argMax(tuple, val) non-empty state.
SELECT finalizeAggregation(CAST(unhex('0101000300000000040000000105000000'), 'AggregateFunction(argMax, Tuple(Nullable(Int32), Nullable(Int32)), Nullable(Int32))'));

-- sumMap(nullable tuple) non-empty state.
SELECT finalizeAggregation(CAST(unhex('0101050700000000000000'), 'AggregateFunction(sumMap, Nullable(Tuple(Array(UInt8), Array(UInt64))))'));

-- sumMappedArrays(nullable tuple) non-empty state.
SELECT finalizeAggregation(CAST(unhex('0101050700000000000000'), 'AggregateFunction(sumMappedArrays, Nullable(Tuple(Array(UInt8), Array(UInt64))))'));

-- sumMapWithOverflow(nullable tuple) non-empty state.
SELECT finalizeAggregation(CAST(unhex('0101050700000000000000'), 'AggregateFunction(sumMapWithOverflow, Nullable(Tuple(Array(UInt8), Array(UInt8))))'));

-- All null states are now different after we have introduced `Nullable(Tuple)`. However the following test will ensure that
-- we can still decode the old all-null states (which had a different layout) without problems, and that the new all-null states are decodable as well.

-- Decode legacy all-null states for nullable signatures.

-- simpleLinearRegression legacy all-null state.
SELECT finalizeAggregation(CAST(unhex('0100000000000000000000000000000000000000000000000000000000000000000000000000000000'), 'AggregateFunction(simpleLinearRegression, Nullable(Float64), Nullable(Float64))'));

-- analysisOfVariance legacy all-null state should keep historical BAD_ARGUMENTS behavior.
SELECT finalizeAggregation(CAST(unhex('01000000'), 'AggregateFunction(analysisOfVariance, Nullable(Float64), Nullable(UInt8))')); -- { serverError BAD_ARGUMENTS }

-- kolmogorovSmirnovTest legacy all-null state should keep historical BAD_ARGUMENTS behavior.
SELECT finalizeAggregation(CAST(unhex('010000'), 'AggregateFunction(kolmogorovSmirnovTest(''two-sided''), Nullable(Float64), Nullable(UInt8))')); -- { serverError BAD_ARGUMENTS }

-- mannWhitneyUTest legacy all-null state should keep historical BAD_ARGUMENTS behavior.
SELECT finalizeAggregation(CAST(unhex('010000'), 'AggregateFunction(mannWhitneyUTest(''two-sided''), Nullable(Float64), Nullable(UInt8))')); -- { serverError BAD_ARGUMENTS }

-- studentTTest legacy all-null state.
SELECT finalizeAggregation(CAST(unhex('01000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'), 'AggregateFunction(studentTTest, Nullable(Float64), Nullable(UInt8))'));

-- welchTTest legacy all-null state.
SELECT finalizeAggregation(CAST(unhex('01000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'), 'AggregateFunction(welchTTest, Nullable(Float64), Nullable(UInt8))'));

-- meanZTest legacy all-null state.
SELECT finalizeAggregation(CAST(unhex('010000000000000000000000000000000000000000000000000000000000000000'), 'AggregateFunction(meanZTest(1., 1., 0.95), Nullable(Float64), Nullable(UInt8))'));

-- studentTTestOneSample legacy all-null state.
SELECT finalizeAggregation(CAST(unhex('010000000000000000000000000000000000000000000000000000000000000000'), 'AggregateFunction(studentTTestOneSample, Nullable(Float64), Nullable(Float64))'));

-- argAndMin legacy all-null state.
SELECT finalizeAggregation(CAST(unhex('010000'), 'AggregateFunction(argAndMin, Nullable(Int32), Nullable(Int32))'));

-- argAndMax legacy all-null state.
SELECT finalizeAggregation(CAST(unhex('010000'), 'AggregateFunction(argAndMax, Nullable(Int32), Nullable(Int32))'));

-- argMin(tuple, val) legacy all-null state.
SELECT finalizeAggregation(CAST(unhex('010000'), 'AggregateFunction(argMin, Tuple(Nullable(Int32), Nullable(Int32)), Nullable(Int32))'));

-- argMax(tuple, val) legacy all-null state.
SELECT finalizeAggregation(CAST(unhex('010000'), 'AggregateFunction(argMax, Tuple(Nullable(Int32), Nullable(Int32)), Nullable(Int32))'));

-- sumMap/sumMappedArrays/sumMapWithOverflow nullable-tuple signatures have no legacy all-null states:
-- older versions did not support Nullable(Tuple(...)) arguments for these functions.

-- Decode legacy tuple-signature states for sumMap/sumMappedArrays/sumMapWithOverflow.
-- These are non-nullable tuple signatures, so `00` below means empty state, not nullable all-null state.

-- sumMap(old tuple signature) non-empty legacy state.
SELECT finalizeAggregation(CAST(unhex('01050700000000000000'), 'AggregateFunction(sumMap, Tuple(Array(UInt8), Array(UInt64)))'));

-- sumMappedArrays(old tuple signature) non-empty legacy state.
SELECT finalizeAggregation(CAST(unhex('01050700000000000000'), 'AggregateFunction(sumMappedArrays, Tuple(Array(UInt8), Array(UInt64)))'));

-- sumMapWithOverflow(old tuple signature) non-empty legacy state.
SELECT finalizeAggregation(CAST(unhex('01050700000000000000'), 'AggregateFunction(sumMapWithOverflow, Tuple(Array(UInt8), Array(UInt8)))'));

-- sumMap(old tuple signature) empty legacy state (non-nullable signature).
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(sumMap, Tuple(Array(UInt8), Array(UInt64)))'));

-- sumMappedArrays(old tuple signature) empty legacy state (non-nullable signature).
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(sumMappedArrays, Tuple(Array(UInt8), Array(UInt64)))'));

-- sumMapWithOverflow(old tuple signature) empty legacy state (non-nullable signature).
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(sumMapWithOverflow, Tuple(Array(UInt8), Array(UInt8)))'));

-- Decode current all-null states.

-- simpleLinearRegression current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(simpleLinearRegression, Nullable(Float64), Nullable(Float64))'));

-- analysisOfVariance current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(analysisOfVariance, Nullable(Float64), Nullable(UInt8))'));

-- kolmogorovSmirnovTest current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(kolmogorovSmirnovTest(''two-sided''), Nullable(Float64), Nullable(UInt8))'));

-- mannWhitneyUTest current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(mannWhitneyUTest(''two-sided''), Nullable(Float64), Nullable(UInt8))'));

-- studentTTest current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(studentTTest, Nullable(Float64), Nullable(UInt8))'));

-- welchTTest current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(welchTTest, Nullable(Float64), Nullable(UInt8))'));

-- meanZTest current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(meanZTest(1., 1., 0.95), Nullable(Float64), Nullable(UInt8))'));

-- studentTTestOneSample current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(studentTTestOneSample, Nullable(Float64), Nullable(Float64))'));

-- argAndMin current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(argAndMin, Nullable(Int32), Nullable(Int32))'));

-- argAndMax current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(argAndMax, Nullable(Int32), Nullable(Int32))'));

-- argMin(tuple, val) current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(argMin, Tuple(Nullable(Int32), Nullable(Int32)), Nullable(Int32))'));

-- argMax(tuple, val) current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(argMax, Tuple(Nullable(Int32), Nullable(Int32)), Nullable(Int32))'));

-- sumMap(nullable tuple) current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(sumMap, Nullable(Tuple(Array(UInt8), Array(UInt64))))'));

-- sumMappedArrays(nullable tuple) current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(sumMappedArrays, Nullable(Tuple(Array(UInt8), Array(UInt64))))'));

-- sumMapWithOverflow(nullable tuple) current all-null state.
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(sumMapWithOverflow, Nullable(Tuple(Array(UInt8), Array(UInt8))))'));

-- studentTTest merge behavior for legacy/current all-null states.

-- Merge legacy all-null state with legacy all-null state.
SELECT studentTTestMerge(st)
FROM
(
    SELECT CAST(unhex('01000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'), 'AggregateFunction(studentTTest, Nullable(Float64), Nullable(UInt8))') AS st
    UNION ALL
    SELECT CAST(unhex('01000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'), 'AggregateFunction(studentTTest, Nullable(Float64), Nullable(UInt8))') AS st
);

-- Merge current all-null state with current all-null state.
SELECT studentTTestMerge(st)
FROM
(
    SELECT CAST(unhex('00'), 'AggregateFunction(studentTTest, Nullable(Float64), Nullable(UInt8))') AS st
    UNION ALL
    SELECT CAST(unhex('00'), 'AggregateFunction(studentTTest, Nullable(Float64), Nullable(UInt8))') AS st
);

-- Merge legacy all-null state with current all-null state.
SELECT studentTTestMerge(st)
FROM
(
    SELECT CAST(unhex('01000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'), 'AggregateFunction(studentTTest, Nullable(Float64), Nullable(UInt8))') AS st
    UNION ALL
    SELECT CAST(unhex('00'), 'AggregateFunction(studentTTest, Nullable(Float64), Nullable(UInt8))') AS st
);

-- Return type drift for Nullable arguments.
-- We return Nullable(Tuple) now for all these functions now which is semantically more correct

-- simpleLinearRegression: legacy type was Tuple(k Float64, b Float64).
SELECT toTypeName(simpleLinearRegression(x, y))
FROM values('x Nullable(Float64), y Nullable(Float64)', (1, 3), (2, 5), (NULL, 7), (4, 9), (5, 11));

-- analysisOfVariance: legacy type was Tuple(f_statistic Float64, p_value Float64).
SELECT toTypeName(analysisOfVariance(v, g))
FROM values('v Nullable(Float64), g Nullable(UInt8)', (1, 0), (3, 0), (5, 0), (2, 1), (10, 1), (NULL, 1));

-- kolmogorovSmirnovTest: legacy type was Tuple(d_statistic Float64, p_value Float64).
SELECT toTypeName(kolmogorovSmirnovTest('two-sided')(v, g))
FROM values('v Nullable(Float64), g Nullable(UInt8)', (1, 0), (3, 0), (5, 0), (2, 1), (10, 1), (NULL, 1));

-- mannWhitneyUTest: legacy type was Tuple(u_statistic Float64, p_value Float64).
SELECT toTypeName(mannWhitneyUTest('two-sided')(v, g))
FROM values('v Nullable(Float64), g Nullable(UInt8)', (1, 0), (3, 0), (5, 0), (2, 1), (10, 1), (NULL, 1));

-- studentTTest: legacy type was Tuple(t_statistic Float64, p_value Float64).
SELECT toTypeName(studentTTest(v, g))
FROM values('v Nullable(Float64), g Nullable(UInt8)', (1, 0), (3, 0), (5, 0), (2, 1), (10, 1), (NULL, 1));

-- welchTTest: legacy type was Tuple(t_statistic Float64, p_value Float64).
SELECT toTypeName(welchTTest(v, g))
FROM values('v Nullable(Float64), g Nullable(UInt8)', (1, 0), (3, 0), (5, 0), (2, 1), (10, 1), (NULL, 1));

-- meanZTest: legacy type was Tuple(z_statistic Float64, p_value Float64, confidence_interval_low Float64, confidence_interval_high Float64).
SELECT toTypeName(meanZTest(1., 1., 0.95)(v, g))
FROM values('v Nullable(Float64), g Nullable(UInt8)', (1, 0), (3, 0), (5, 0), (2, 1), (10, 1), (NULL, 1));

-- studentTTestOneSample: legacy type was Tuple(t_statistic Float64, p_value Float64).
SELECT toTypeName(studentTTestOneSample(v, m))
FROM values('v Nullable(Float64), m Nullable(Float64)', (1, 2), (3, 2), (NULL, 2), (5, 2));

-- argAndMin: legacy type was Tuple(Int32, Int32).
SELECT toTypeName(argAndMin(a, b))
FROM values('a Nullable(Int32), b Nullable(Int32)', (1, 2), (NULL, 1), (3, 0));

-- argAndMax: legacy type was Tuple(Int32, Int32).
SELECT toTypeName(argAndMax(a, b))
FROM values('a Nullable(Int32), b Nullable(Int32)', (1, 2), (NULL, 1), (3, 0));

-- argMin(tuple, val): legacy type was Tuple(Nullable(Int32), Nullable(Int32)).
SELECT toTypeName(argMin(tuple(a, b), c))
FROM values('a Nullable(Int32), b Nullable(Int32), c Nullable(Int32)', (1, 2, 3), (NULL, 1, 0), (3, 4, 5));

-- argMax(tuple, val): legacy type was Tuple(Nullable(Int32), Nullable(Int32)).
SELECT toTypeName(argMax(tuple(a, b), c))
FROM values('a Nullable(Int32), b Nullable(Int32), c Nullable(Int32)', (1, 2, 3), (NULL, 1, 0), (3, 4, 5));

-- sumMap(nullable tuple): legacy type was Tuple(Array(UInt8), Array(UInt64)).
SELECT toTypeName(finalizeAggregation(CAST(unhex('0101050700000000000000'), 'AggregateFunction(sumMap, Nullable(Tuple(Array(UInt8), Array(UInt64))))')));

-- sumMappedArrays(nullable tuple): legacy type was Tuple(Array(UInt8), Array(UInt64)).
SELECT toTypeName(finalizeAggregation(CAST(unhex('0101050700000000000000'), 'AggregateFunction(sumMappedArrays, Nullable(Tuple(Array(UInt8), Array(UInt64))))')));

-- sumMapWithOverflow(nullable tuple): legacy type was Tuple(Array(UInt8), Array(UInt8)).
SELECT toTypeName(finalizeAggregation(CAST(unhex('0101050700000000000000'), 'AggregateFunction(sumMapWithOverflow, Nullable(Tuple(Array(UInt8), Array(UInt8))))')));
