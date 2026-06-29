-- { echo }

-- Backward compatibility test for Tuple-returning aggregate functions with combinators and Nullable arguments.
-- These tests verify that old states (from v25.11, pre-Nullable(Tuple)) can still be deserialized,
-- and that new states match the old format (round-trip).

-- If combinator: deserialize old states and round-trip.

-- 1. simpleLinearRegressionIf(Nullable(Float64), Nullable(Float64), cond)
SELECT finalizeAggregation(CAST(unhex('03000000000000000000000000002640000000000000394000000000008046400000000000405940'), 'AggregateFunction(simpleLinearRegressionIf, Nullable(Float64), Nullable(Float64), UInt8)'));

SELECT hex(simpleLinearRegressionIfState(x, y, x > 1))
FROM values('x Nullable(Float64), y Nullable(Float64)', (1, 3), (2, 5), (NULL, 7), (4, 9), (5, 11));

-- 2. studentTTestIf(Nullable(Float64), Nullable(UInt8), cond)
SELECT tuple(roundBankers(res.1, 4), roundBankers(res.2, 4))
FROM (SELECT finalizeAggregation(CAST(unhex('000000000000004000000000000000400000000000002040000000000000284000000000000041400000000000005A40'), 'AggregateFunction(studentTTestIf, Nullable(Float64), Nullable(UInt8), UInt8)')) AS res);

SELECT hex(studentTTestIfState(v, g, v > 1))
FROM values('v Nullable(Float64), g Nullable(UInt8)', (1, 0), (3, 0), (5, 0), (2, 1), (10, 1), (NULL, 1));

-- 3. welchTTestIf(Nullable(Float64), Nullable(UInt8), cond)
SELECT tuple(roundBankers(res.1, 4), roundBankers(res.2, 4))
FROM (SELECT finalizeAggregation(CAST(unhex('000000000000004000000000000000400000000000002040000000000000284000000000000041400000000000005A40'), 'AggregateFunction(welchTTestIf, Nullable(Float64), Nullable(UInt8), UInt8)')) AS res);

SELECT hex(welchTTestIfState(v, g, v > 1))
FROM values('v Nullable(Float64), g Nullable(UInt8)', (1, 0), (3, 0), (5, 0), (2, 1), (10, 1), (NULL, 1));

-- 4. meanZTestIf(1., 1., 0.95)(Nullable(Float64), Nullable(UInt8), cond)
SELECT tuple(roundBankers(res.1, 4), roundBankers(res.2, 4), roundBankers(res.3, 4), roundBankers(res.4, 4))
FROM (SELECT finalizeAggregation(CAST(unhex('0000000000000040000000000000004000000000000020400000000000002840'), 'AggregateFunction(meanZTestIf(1., 1., 0.95), Nullable(Float64), Nullable(UInt8), UInt8)')) AS res);

SELECT hex(meanZTestIfState(1., 1., 0.95)(v, g, v > 1))
FROM values('v Nullable(Float64), g Nullable(UInt8)', (1, 0), (3, 0), (5, 0), (2, 1), (10, 1), (NULL, 1));

-- 5. argAndMinIf(Nullable(Int32), Nullable(Int32), cond)
SELECT finalizeAggregation(CAST(unhex('01030000000100000000'), 'AggregateFunction(argAndMinIf, Nullable(Int32), Nullable(Int32), UInt8)'));

SELECT hex(argAndMinIfState(a, b, a > 0))
FROM values('a Nullable(Int32), b Nullable(Int32)', (1, 2), (NULL, 1), (3, 0));

-- 6. argAndMaxIf(Nullable(Int32), Nullable(Int32), cond)
SELECT finalizeAggregation(CAST(unhex('01010000000102000000'), 'AggregateFunction(argAndMaxIf, Nullable(Int32), Nullable(Int32), UInt8)'));

SELECT hex(argAndMaxIfState(a, b, a > 0))
FROM values('a Nullable(Int32), b Nullable(Int32)', (1, 2), (NULL, 1), (3, 0));

-- 7. kolmogorovSmirnovTestIf('two-sided')(Nullable(Float64), Nullable(UInt8), cond)
SELECT tuple(roundBankers(res.1, 4), roundBankers(res.2, 4))
FROM (SELECT finalizeAggregation(CAST(unhex('02020000000000000840000000000000144000000000000000400000000000002440'), 'AggregateFunction(kolmogorovSmirnovTestIf(''two-sided''), Nullable(Float64), Nullable(UInt8), UInt8)')) AS res);

SELECT hex(kolmogorovSmirnovTestIfState('two-sided')(v, g, v > 1))
FROM values('v Nullable(Float64), g Nullable(UInt8)', (1, 0), (3, 0), (5, 0), (2, 1), (10, 1), (NULL, 1));

-- Distinct combinator: deserialize old states and round-trip.

-- simpleLinearRegressionDistinct
SELECT finalizeAggregation(CAST(unhex('01041000000000000010400000000000002240100000000000000040000000000000144010000000000000F03F00000000000008401000000000000014400000000000002640'), 'AggregateFunction(simpleLinearRegressionDistinct, Nullable(Float64), Nullable(Float64))'));

SELECT hex(simpleLinearRegressionDistinctState(x, y))
FROM values('x Nullable(Float64), y Nullable(Float64)', (1, 3), (2, 5), (NULL, 7), (4, 9), (5, 11));

-- studentTTestDistinct
SELECT tuple(roundBankers(res.1, 4), roundBankers(res.2, 4))
FROM (SELECT finalizeAggregation(CAST(unhex('010509000000000000F03F0009000000000000244001090000000000000840000900000000000014400009000000000000004001'), 'AggregateFunction(studentTTestDistinct, Nullable(Float64), Nullable(UInt8))')) AS res);

SELECT hex(studentTTestDistinctState(v, g))
FROM values('v Nullable(Float64), g Nullable(UInt8)', (1, 0), (3, 0), (5, 0), (2, 1), (10, 1), (NULL, 1));

-- argAndMinDistinct
SELECT finalizeAggregation(CAST(unhex('0102080100000002000000080300000000000000'), 'AggregateFunction(argAndMinDistinct, Nullable(Int32), Nullable(Int32))'));

SELECT hex(argAndMinDistinctState(a, b))
FROM values('a Nullable(Int32), b Nullable(Int32)', (1, 2), (NULL, 1), (3, 0));

-- Merge combinator: deserialize old states and round-trip.

-- simpleLinearRegressionMerge
SELECT finalizeAggregation(CAST(unhex('01040000000000000000000000000028400000000000003C4000000000000047400000000000005A40'), 'AggregateFunction(simpleLinearRegressionMerge, AggregateFunction(simpleLinearRegression, Nullable(Float64), Nullable(Float64)))'));

SELECT hex(simpleLinearRegressionMergeState(s))
FROM (SELECT simpleLinearRegressionState(x, y) AS s FROM values('x Nullable(Float64), y Nullable(Float64)', (1, 3), (2, 5), (4, 9), (5, 11)));

-- argAndMinMerge
SELECT finalizeAggregation(CAST(unhex('0101030000000100000000'), 'AggregateFunction(argAndMinMerge, AggregateFunction(argAndMin, Nullable(Int32), Nullable(Int32)))'));

SELECT hex(argAndMinMergeState(s))
FROM (SELECT argAndMinState(a, b) AS s FROM values('a Nullable(Int32), b Nullable(Int32)', (1, 2), (3, 0)));

-- OrDefault combinator: deserialize old states and round-trip.

-- simpleLinearRegressionOrDefault
SELECT finalizeAggregation(CAST(unhex('01040000000000000000000000000028400000000000003C4000000000000047400000000000005A4001'), 'AggregateFunction(simpleLinearRegressionOrDefault, Nullable(Float64), Nullable(Float64))'));

SELECT hex(simpleLinearRegressionOrDefaultState(x, y))
FROM values('x Nullable(Float64), y Nullable(Float64)', (1, 3), (2, 5), (NULL, 7), (4, 9), (5, 11));

-- argAndMinOrDefault
SELECT finalizeAggregation(CAST(unhex('010103000000010000000001'), 'AggregateFunction(argAndMinOrDefault, Nullable(Int32), Nullable(Int32))'));

SELECT hex(argAndMinOrDefaultState(a, b))
FROM values('a Nullable(Int32), b Nullable(Int32)', (1, 2), (NULL, 1), (3, 0));
