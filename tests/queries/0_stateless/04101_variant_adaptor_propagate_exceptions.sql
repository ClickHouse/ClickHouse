-- Tags: no-random-settings
-- Test that non-type-conversion exceptions (like MEMORY_LIMIT_EXCEEDED) are propagated
-- from FunctionVariantAdaptor instead of being incorrectly wrapped as LOGICAL_ERROR.
-- See https://github.com/ClickHouse/ClickHouse/issues/93960

SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

-- Path 1: Single variant type, no NULLs → castColumn for Variant result
DROP TABLE IF EXISTS test_variant_oom;
CREATE TABLE test_variant_oom (v Variant(Float64, Int64)) ENGINE = Memory;
INSERT INTO test_variant_oom SELECT (number * 1.0001)::Float64::Variant(Float64, Int64) FROM numbers(5000000);

SELECT moduloOrZero(v, 65536) FROM test_variant_oom SETTINGS max_memory_usage = 10000000 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Path 2: Single variant type + NULLs → castColumn for Variant result with filter
DROP TABLE IF EXISTS test_variant_oom2;
CREATE TABLE test_variant_oom2 (v Variant(Float64, Int64)) ENGINE = Memory;
INSERT INTO test_variant_oom2 SELECT if(number % 3 = 0, NULL, (number * 1.0001)::Float64)::Variant(Float64, Int64) FROM numbers(5000000);

SELECT moduloOrZero(v, 65536) FROM test_variant_oom2 SETTINGS max_memory_usage = 10000000 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Path 3: Multiple variant types → general castColumn path
DROP TABLE IF EXISTS test_variant_oom3;
CREATE TABLE test_variant_oom3 (v Variant(Float64, Int64)) ENGINE = Memory;
INSERT INTO test_variant_oom3 SELECT if(number % 2 = 0, (number * 1.0001)::Float64, number::Int64)::Variant(Float64, Int64) FROM numbers(5000000);

SELECT moduloOrZero(v, 65536) FROM test_variant_oom3 SETTINGS max_memory_usage = 10000000 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Verify normal operation works fine
SELECT count(moduloOrZero(v, 65536)) FROM test_variant_oom FORMAT Null;
SELECT count(moduloOrZero(v, 65536)) FROM test_variant_oom2 FORMAT Null;
SELECT count(moduloOrZero(v, 65536)) FROM test_variant_oom3 FORMAT Null;

DROP TABLE test_variant_oom;
DROP TABLE test_variant_oom2;
DROP TABLE test_variant_oom3;
