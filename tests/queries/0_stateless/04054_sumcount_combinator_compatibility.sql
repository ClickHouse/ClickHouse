-- { echo }

-- Backward compatibility test for sumCount with all combinators and Nullable arguments.
-- After Nullable(Tuple) was introduced, the Null/If combinators changed serialization for
-- Tuple-returning functions. These tests verify that old states (from v25.11, pre-Nullable(Tuple))
-- can still be deserialized, and that new states match the old format byte-for-byte.

SET allow_suspicious_low_cardinality_types = 1;

-- sumCountIf: Nullable and non-Nullable produce the same state format (no flag byte).
SELECT hex(sumCountIfState(x, x > 0)) FROM (SELECT CAST(number, 'UInt8') AS x FROM numbers(5));
SELECT hex(sumCountIfState(x, x > 0)) FROM (SELECT CAST(number, 'Nullable(UInt8)') AS x FROM numbers(5));

-- sumCountIf: return type is Tuple, not Nullable(Tuple).
SELECT toTypeName(sumCountIf(x, x > 0)) FROM (SELECT CAST(number, 'Nullable(UInt8)') AS x FROM numbers(5));

-- sumCountIf: deserialize old states.
SELECT finalizeAggregation(CAST(unhex('070000000000000002'), 'AggregateFunction(sumCountIf, Nullable(UInt8), UInt8)'));
SELECT finalizeAggregation(CAST(unhex('000000000000000000'), 'AggregateFunction(sumCountIf, Nullable(UInt8), UInt8)'));

-- sumCountIf: round-trip produces identical bytes.
SELECT hex(sumCountIfState(x, x > 2)) FROM (SELECT number::Nullable(UInt8) AS x FROM numbers(5));
SELECT hex(sumCountIfState(x, 0)) FROM (SELECT CAST(NULL, 'Nullable(UInt8)') AS x);
SELECT hex(sumCountIfState(x, 1)) FROM (SELECT CAST(NULL, 'Nullable(UInt8)') AS x);

-- sumCountIf: LowCardinality(Nullable) same format.
SELECT hex(sumCountIfState(x, x > 2)) FROM (SELECT CAST(number, 'LowCardinality(Nullable(UInt8))') AS x FROM numbers(5));

-- sumCountIf: merge old states.
SELECT sumCountIfMerge(st) FROM
(
    SELECT CAST(unhex('070000000000000002'), 'AggregateFunction(sumCountIf, Nullable(UInt8), UInt8)') AS st
    UNION ALL
    SELECT CAST(unhex('070000000000000002'), 'AggregateFunction(sumCountIf, Nullable(UInt8), UInt8)') AS st
);

-- sumCountDistinct: deserialize old states.
SELECT finalizeAggregation(CAST(unhex('0404020103'), 'AggregateFunction(sumCountDistinct, Nullable(UInt8))'));
SELECT finalizeAggregation(CAST(unhex('00'), 'AggregateFunction(sumCountDistinct, Nullable(UInt8))'));

-- sumCountDistinct: round-trip.
SELECT hex(sumCountDistinctState(x)) FROM (SELECT number::Nullable(UInt8) AS x FROM numbers(5) WHERE x > 0);
SELECT hex(sumCountDistinctState(x)) FROM (SELECT CAST(1, 'Nullable(UInt8)') AS x WHERE 0);
SELECT hex(sumCountDistinctState(x)) FROM (SELECT CAST(NULL, 'Nullable(UInt8)') AS x);

-- sumCountDistinct: LowCardinality(Nullable) same format.
SELECT hex(sumCountDistinctState(x)) FROM (SELECT CAST(number, 'LowCardinality(Nullable(UInt8))') AS x FROM numbers(5) WHERE x > 0);

-- sumCountDistinctIf: deserialize old state and round-trip.
SELECT finalizeAggregation(CAST(unhex('03040203'), 'AggregateFunction(sumCountDistinctIf, Nullable(UInt8), UInt8)'));
SELECT hex(sumCountDistinctIfState(x, x > 1)) FROM (SELECT number::Nullable(UInt8) AS x FROM numbers(5));

-- sumCountArray: deserialize old state and round-trip.
SELECT finalizeAggregation(CAST(unhex('0A0000000000000004'), 'AggregateFunction(sumCountArray, Array(Nullable(UInt8)))'));
SELECT hex(sumCountArrayState(x)) FROM (SELECT [number::Nullable(UInt8)] AS x FROM numbers(5) WHERE number > 0);
SELECT finalizeAggregation(CAST(unhex('000000000000000000'), 'AggregateFunction(sumCountArray, Array(Nullable(UInt8)))'));

-- sumCountForEach: deserialize old state and round-trip.
SELECT finalizeAggregation(CAST(unhex('01000000000000000A0000000000000004'), 'AggregateFunction(sumCountForEach, Array(Nullable(UInt8)))'));
SELECT hex(sumCountForEachState(x)) FROM (SELECT [number::Nullable(UInt8)] AS x FROM numbers(5) WHERE number > 0);

-- sumCountMerge: deserialize old state and round-trip.
SELECT finalizeAggregation(CAST(unhex('0A0000000000000004'), 'AggregateFunction(sumCountMerge, AggregateFunction(sumCount, Nullable(UInt8)))'));
SELECT hex(sumCountMergeState(s)) FROM (SELECT sumCountState(number::Nullable(UInt8)) AS s FROM numbers(5) WHERE number > 0);

-- sumCountResample: deserialize old state and round-trip.
SELECT finalizeAggregation(CAST(unhex('01000000000000000001010000000000000001020000000000000001030000000000000001040000000000000001'), 'AggregateFunction(sumCountResample(0, 5, 1), Nullable(UInt8), UInt8)'));
SELECT hex(sumCountResampleState(0, 5, 1)(x, y)) FROM (SELECT number::Nullable(UInt8) AS x, number::UInt8 AS y FROM numbers(5));

-- sumCountOrDefault: deserialize old state and round-trip.
SELECT finalizeAggregation(CAST(unhex('0A000000000000000501'), 'AggregateFunction(sumCountOrDefault, Nullable(UInt8))'));
SELECT hex(sumCountOrDefaultState(x)) FROM (SELECT number::Nullable(UInt8) AS x FROM numbers(5));
SELECT finalizeAggregation(CAST(unhex('00000000000000000000'), 'AggregateFunction(sumCountOrDefault, Nullable(UInt8))'));
SELECT hex(sumCountOrDefaultState(x)) FROM (SELECT CAST(1, 'Nullable(UInt8)') AS x WHERE 0);

-- sumCountOrDefault: LowCardinality(Nullable) same format.
SELECT hex(sumCountOrDefaultState(x)) FROM (SELECT CAST(number, 'LowCardinality(Nullable(UInt8))') AS x FROM numbers(5));

-- sumCountOrNull: round-trip (no backward compat concern — didn't exist pre-Nullable(Tuple)).
SELECT finalizeAggregation(CAST(unhex('0A000000000000000501'), 'AggregateFunction(sumCountOrNull, Nullable(UInt8))'));
SELECT hex(sumCountOrNullState(x)) FROM (SELECT number::Nullable(UInt8) AS x FROM numbers(5));

SELECT sumCountOrNull(x) FROM (SELECT number::Nullable(UInt8) AS x FROM numbers(5) WHERE x > 0);
SELECT sumCountOrDefault(x) FROM (SELECT number::Nullable(UInt8) AS x FROM numbers(5) WHERE x > 0);
