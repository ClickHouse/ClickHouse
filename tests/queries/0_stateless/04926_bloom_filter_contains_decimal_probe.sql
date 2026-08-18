-- Decimal probes use the same accurate numeric conversion as integer and floating-point probes.
WITH
    (SELECT groupBloomFilterState(100)(toUInt64(42)) FROM numbers(1)) AS uint64_bf,
    (SELECT groupBloomFilterState(100)(toFloat64(42.5)) FROM numbers(1)) AS float64_bf
SELECT
    bloomFilterContains(uint64_bf, toDecimal32(42, 0)),
    bloomFilterContains(uint64_bf, toDecimal64(42, 0)),
    bloomFilterContains(uint64_bf, toDecimal128(42, 0)),
    bloomFilterContains(uint64_bf, toDecimal256(42, 0)),
    bloomFilterContains(float64_bf, toDecimal64(42.5, 1));
