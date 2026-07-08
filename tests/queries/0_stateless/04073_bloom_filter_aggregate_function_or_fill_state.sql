-- State-only `OrNull`/`OrDefault` chains must be constructible for `groupBloomFilter`,
-- even though finalized `groupBloomFilterOrNull`/`groupBloomFilterOrDefault` throw.
SELECT toTypeName(groupBloomFilterOrNullState(1000)(number)) LIKE 'AggregateFunction(1, groupBloomFilterOrNull%' FROM numbers(1);
SELECT toTypeName(groupBloomFilterOrDefaultState(1000)(number)) LIKE 'AggregateFunction(1, groupBloomFilterOrDefault%' FROM numbers(1);

-- Empty/skipped inputs must still return an aggregate state, not hit the finalized default path.
SELECT length(hex(groupBloomFilterOrNullState(1000)(number))) > 0 FROM numbers(0);
SELECT length(hex(groupBloomFilterIfOrDefaultState(1000)(number, 0))) > 0 FROM numbers(1);

-- `OrNull`/`OrDefault` append their flag after the nested state, so `bloomFilterContains`
-- must still be able to probe the nested `groupBloomFilter` state.
WITH
    (SELECT groupBloomFilterOrNullState(4096, 5, 0)(number) FROM numbers(100)) AS or_null_bf,
    (SELECT groupBloomFilterOrDefaultState(4096, 5, 0)(number) FROM numbers(100)) AS or_default_bf
SELECT
    bloomFilterContains(or_null_bf, toUInt64(42)),
    bloomFilterContains(or_default_bf, toUInt64(42));

WITH
    (SELECT groupBloomFilterIfOrNullState(4096, 5, 0)(number, number = 42) FROM numbers(100)) AS if_or_null_bf,
    (SELECT groupBloomFilterIfOrDefaultState(4096, 5, 0)(number, number = 42) FROM numbers(100)) AS if_or_default_bf
SELECT
    bloomFilterContains(if_or_null_bf, toUInt64(42)),
    bloomFilterContains(if_or_null_bf, toUInt64(41)),
    bloomFilterContains(if_or_default_bf, toUInt64(42)),
    bloomFilterContains(if_or_default_bf, toUInt64(41));

WITH
    (SELECT groupBloomFilterOrNullState(4096, 5, 0)(number) FROM numbers(0)) AS empty_or_null_bf,
    (SELECT groupBloomFilterIfOrDefaultState(4096, 5, 0)(number, 0) FROM numbers(100)) AS skipped_or_default_bf
SELECT
    bloomFilterContains(empty_or_null_bf, toUInt64(42)),
    bloomFilterContains(skipped_or_default_bf, toUInt64(42));
