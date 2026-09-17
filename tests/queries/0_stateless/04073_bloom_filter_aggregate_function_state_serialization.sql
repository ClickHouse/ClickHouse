-- Validation and round trips for serialized `groupBloomFilter` aggregate states.

-- Malformed serialized state: `filter_size_bytes` is zero.
SELECT finalizeAggregation(CAST(unhex('00010000'), 'AggregateFunction(1, groupBloomFilter(1000), UInt64)')); -- { serverError INCORRECT_DATA }

-- Malformed serialized state: `filter_size_bytes` exceeds the maximum.
SELECT finalizeAggregation(CAST(unhex('8180808001010000'), 'AggregateFunction(1, groupBloomFilter(1000), UInt64)')); -- { serverError INCORRECT_DATA }

-- Malformed serialized state: `num_hashes` is zero.
SELECT finalizeAggregation(CAST(unhex('08000000'), 'AggregateFunction(1, groupBloomFilter(1000), UInt64)')); -- { serverError INCORRECT_DATA }

-- Malformed serialized state: `num_hashes` exceeds the maximum.
SELECT finalizeAggregation(CAST(unhex('08150000'), 'AggregateFunction(1, groupBloomFilter(1000), UInt64)')); -- { serverError INCORRECT_DATA }

-- Malformed serialized state: `has_data` must be zero or one.
SELECT finalizeAggregation(CAST(unhex('08010002'), 'AggregateFunction(1, groupBloomFilter(1000), UInt64)')); -- { serverError INCORRECT_DATA }

-- Valid payload parameters must match the declared aggregate type.
SELECT CAST(unhex('08010000'), 'AggregateFunction(1, groupBloomFilter(1000), UInt64)'); -- { serverError INCORRECT_DATA }

-- The serialized seed must match the declared aggregate type.
SELECT CAST(unhex('8020052a00'), 'AggregateFunction(1, groupBloomFilter(4096, 5, 0), UInt64)'); -- { serverError INCORRECT_DATA }

-- Empty and skipped states stay compact and return zero from `bloomFilterContains`.
WITH
    (SELECT groupBloomFilterState(1000)(number) FROM numbers(0)) AS empty_bf,
    (SELECT groupBloomFilterState(1000)(materialize(CAST(NULL, 'Nullable(UInt64)'))) FROM numbers(100)) AS all_null_bf,
    (SELECT groupBloomFilterState(1000)(number) FROM numbers(100)) AS full_bf
SELECT
    bloomFilterContains(empty_bf, toUInt64(42)),
    bloomFilterContains(all_null_bf, toUInt64(42)),
    length(CAST(empty_bf AS String)) < length(CAST(full_bf AS String)),
    length(CAST(all_null_bf AS String)) < length(CAST(full_bf AS String));

-- Revision-0 `Native` files use the original v1 aggregate-state layout.
INSERT INTO FUNCTION file(currentDatabase() || '_04073_bloom_filter_native.native', 'Native', 'bf AggregateFunction(groupBloomFilter(1000), UInt64), skipped AggregateFunction(groupBloomFilterIf(1000), UInt64, UInt8)')
SELECT
    groupBloomFilterState(1000)(number) AS bf,
    groupBloomFilterIfState(1000)(number, 0) AS skipped
FROM numbers(100)
SETTINGS engine_file_truncate_on_insert = 1;

SELECT
    bloomFilterContains(bf, toUInt64(42)),
    bloomFilterContains(bf, toUInt64(200)),
    bloomFilterContains(skipped, toUInt64(42))
FROM file(currentDatabase() || '_04073_bloom_filter_native.native', 'Native', 'bf AggregateFunction(groupBloomFilter(1000), UInt64), skipped AggregateFunction(groupBloomFilterIf(1000), UInt64, UInt8)');
