-- Regression test for crash: "ColumnUnique can't contain null values"
-- when has() is used with PREWHERE on a Tuple key containing LowCardinality elements.
-- The crash occurred because tryPrepareSetColumnsForIndex passed the LowCardinality-wrapped
-- key type to castColumnAccurateOrNull, which produced null values for out-of-range casts
-- that were then inserted into a non-nullable LowCardinality dictionary.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS test_has_lc_tuple_crash;

CREATE TABLE test_has_lc_tuple_crash
(
    id UInt64,
    key_tuple Tuple(LowCardinality(UInt32), UInt32),
    payload UInt64
)
ENGINE = MergeTree
ORDER BY key_tuple
SETTINGS index_granularity = 1000, allow_nullable_key = 1;

INSERT INTO test_has_lc_tuple_crash SELECT number, (number, number % 10), number FROM numbers(1000);

-- This query should not crash. The Int64 values (-2147483649, 9223372036854775806)
-- cannot be safely cast to (LowCardinality(UInt32), UInt32), so accurateOrNull produces nulls.
-- Previously, these nulls were inserted into the non-nullable LowCardinality dictionary, causing a crash.
SELECT count() FROM test_has_lc_tuple_crash
    PREWHERE has((SELECT DISTINCT [(-2147483649, 9223372036854775806)]), key_tuple)
    WHERE has((SELECT DISTINCT [(1, 10)]), key_tuple);

-- Simpler variant: just PREWHERE with out-of-range values
SELECT count() FROM test_has_lc_tuple_crash
    PREWHERE has([(-1, 0)], key_tuple);

-- Verify correct results still work: (10, 0) matches row where number=10 (key_tuple=(10, 10%10)=(10,0))
SELECT count() FROM test_has_lc_tuple_crash
    WHERE has([(10, 0)], key_tuple);

DROP TABLE test_has_lc_tuple_crash;
