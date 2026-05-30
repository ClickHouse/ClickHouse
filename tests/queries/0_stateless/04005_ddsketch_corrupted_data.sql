-- Test that corrupted DDSketch binary data is rejected during deserialization.
-- Corrupted data should throw INCORRECT_DATA, not cause segfaults, OOM, or infinite loops.

-- Corrupted gamma: infinity
SELECT quantilesDDMerge(0.01, 0.5)(d) FROM (SELECT unhex('02000000000000F07F0000000000000000010C000002030C000002040000000000000000')::AggregateFunction(quantilesDD(0.01, 0.5), Float64) AS d); -- { serverError INCORRECT_DATA }

-- Corrupted gamma: NaN
SELECT quantilesDDMerge(0.01, 0.5)(d) FROM (SELECT unhex('02000000000000F87F0000000000000000010C000002030C000002040000000000000000')::AggregateFunction(quantilesDD(0.01, 0.5), Float64) AS d); -- { serverError INCORRECT_DATA }

-- Corrupted gamma: equal to 1.0
SELECT quantilesDDMerge(0.01, 0.5)(d) FROM (SELECT unhex('02000000000000F03F0000000000000000010C000002030C000002040000000000000000')::AggregateFunction(quantilesDD(0.01, 0.5), Float64) AS d); -- { serverError INCORRECT_DATA }

-- Corrupted gamma: less than 1.0
SELECT quantilesDDMerge(0.01, 0.5)(d) FROM (SELECT unhex('02000000000000E03F0000000000000000010C000002030C000002040000000000000000')::AggregateFunction(quantilesDD(0.01, 0.5), Float64) AS d); -- { serverError INCORRECT_DATA }

-- Corrupted offset: NaN
SELECT quantilesDDMerge(0.01, 0.5)(d) FROM (SELECT unhex('020000000000000040000000000000F87F010C000002030C000002040000000000000000')::AggregateFunction(quantilesDD(0.01, 0.5), Float64) AS d); -- { serverError INCORRECT_DATA }

-- Corrupted zero_count: NaN
SELECT quantilesDDMerge(0.01, 0.5)(d) FROM (SELECT unhex('0200000000000000400000000000000000010C000002030C00000204000000000000F87F')::AggregateFunction(quantilesDD(0.01, 0.5), Float64) AS d); -- { serverError INCORRECT_DATA }

-- Corrupted zero_count: negative
SELECT quantilesDDMerge(0.01, 0.5)(d) FROM (SELECT unhex('0200000000000000400000000000000000010C000002030C00000204000000000000F0BF')::AggregateFunction(quantilesDD(0.01, 0.5), Float64) AS d); -- { serverError INCORRECT_DATA }

-- Corrupted bin count: NaN
SELECT quantilesDDMerge(0.01, 0.5)(d) FROM (SELECT unhex('020000000000000040000000000000000001040100000000000000F87F030C000002040000000000000000')::AggregateFunction(quantilesDD(0.01, 0.5), Float64) AS d); -- { serverError INCORRECT_DATA }

-- Corrupted bin count: negative
SELECT quantilesDDMerge(0.01, 0.5)(d) FROM (SELECT unhex('020000000000000040000000000000000001040100000000000000F0BF030C000002040000000000000000')::AggregateFunction(quantilesDD(0.01, 0.5), Float64) AS d); -- { serverError INCORRECT_DATA }

-- Valid payload: gamma=2.0, zero_count=5.0, no bins
SELECT quantilesDDMerge(0.01, 0.5)(d) FROM (SELECT unhex('0200000000000000400000000000000000010C000002030C000002040000000000001440')::AggregateFunction(quantilesDD(0.01, 0.5), Float64) AS d);
