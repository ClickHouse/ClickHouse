-- State-only `OrNull`/`OrDefault` chains must be constructible for `groupBloomFilter`,
-- even though finalized `groupBloomFilterOrNull`/`groupBloomFilterOrDefault` throw.
SELECT toTypeName(groupBloomFilterOrNullState(1000)(number)) LIKE 'AggregateFunction(1, groupBloomFilterOrNull%' FROM numbers(1);
SELECT toTypeName(groupBloomFilterOrDefaultState(1000)(number)) LIKE 'AggregateFunction(1, groupBloomFilterOrDefault%' FROM numbers(1);

-- Empty/skipped inputs must still return an aggregate state, not hit the finalized default path.
SELECT length(hex(groupBloomFilterOrNullState(1000)(number))) > 0 FROM numbers(0);
SELECT length(hex(groupBloomFilterIfOrDefaultState(1000)(number, 0))) > 0 FROM numbers(1);
