-- Nullable arguments require a null adapter whose state is not interchangeable with
-- the state of `groupBloomFilterIf` or nested variants such as `groupBloomFilterArrayIf`.
SELECT groupBloomFilterIfState(CAST(number, 'Nullable(UInt64)'), toUInt8(1))
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterIfState(number, CAST(1, 'Nullable(UInt8)'))
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterArrayIfState([number], CAST(1, 'Nullable(UInt8)'))
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

-- `OrNull` and `OrDefault` require a meaningful finalized result, which
-- `groupBloomFilter` intentionally does not provide, even in state-only chains.
SELECT groupBloomFilterOrNullState(1000)(number)
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterOrDefaultState(1000)(number)
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterIfOrDefaultState(1000)(number, toUInt8(1))
FROM numbers(1); -- { serverError BAD_ARGUMENTS }
