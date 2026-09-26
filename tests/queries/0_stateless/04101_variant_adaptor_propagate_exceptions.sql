-- Tags: no-random-settings
-- Test that non-type-conversion exceptions (like MEMORY_LIMIT_EXCEEDED) are propagated
-- from FunctionVariantAdaptor instead of being incorrectly wrapped as LOGICAL_ERROR.
-- See https://github.com/ClickHouse/ClickHouse/issues/93960

SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

-- Path 1: Single variant type, no NULLs → castColumn for Variant result
DROP TABLE IF EXISTS test_variant_oom;
CREATE TABLE test_variant_oom (v Variant(Float64, Int64)) ENGINE = Memory;
INSERT INTO test_variant_oom SELECT (number * 1.0001)::Float64::Variant(Float64, Int64) FROM numbers(5000000) SETTINGS max_block_size = 57000;

-- The pins above and below make the peak one stored `Variant` chunk, and all four are required.
-- `max_block_size` was verified to keep every throw inside the guarded `castColumn` over
-- [50000, 65409]; retuning it outside that range can move the throw out, or stop it happening.
SELECT moduloOrZero(v, 65536) FROM test_variant_oom SETTINGS max_memory_usage = 2000000, max_threads = 1, max_untracked_memory = 0 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Path 2: Single variant type + NULLs → castColumn for Variant result with filter
DROP TABLE IF EXISTS test_variant_oom2;
CREATE TABLE test_variant_oom2 (v Variant(Float64, Int64)) ENGINE = Memory;
INSERT INTO test_variant_oom2 SELECT if(number % 3 = 0, NULL, (number * 1.0001)::Float64)::Variant(Float64, Int64) FROM numbers(5000000) SETTINGS max_block_size = 57000;

SELECT moduloOrZero(v, 65536) FROM test_variant_oom2 SETTINGS max_memory_usage = 2000000, max_threads = 1, max_untracked_memory = 0 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Path 3: Multiple variant types → general `castColumn` path. `abs` of `Int64` and of `UInt64` are
-- both `UInt64`, and that duplicate result type is what makes direct `Variant` construction
-- inapplicable; `moduloOrZero` yields two distinct types and is built directly, reaching no cast.
DROP TABLE IF EXISTS test_variant_oom3;
CREATE TABLE test_variant_oom3 (v Variant(Int64, UInt64, Float64)) ENGINE = Memory;
INSERT INTO test_variant_oom3 SELECT multiIf(number % 3 = 0, (-number)::Int64::Variant(Int64, UInt64, Float64), number % 3 = 1, number::UInt64::Variant(Int64, UInt64, Float64), (number * 1.5)::Float64::Variant(Int64, UInt64, Float64)) FROM numbers(5000000) SETTINGS max_block_size = 57000;

SELECT abs(v) FROM test_variant_oom3 SETTINGS max_memory_usage = 2000000, max_threads = 1, max_untracked_memory = 0 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Each pinned INSERT must store ceil(5000000 / 57000) = 88 blocks. Dropping or overriding a pin
-- changes this count, which is what keeps the assertions above from silently ceasing to assert.
SELECT count() FROM (SELECT 1 FROM test_variant_oom GROUP BY blockNumber());
SELECT count() FROM (SELECT 1 FROM test_variant_oom2 GROUP BY blockNumber());
SELECT count() FROM (SELECT 1 FROM test_variant_oom3 GROUP BY blockNumber());

-- Verify normal operation works fine
SELECT count(moduloOrZero(v, 65536)) FROM test_variant_oom FORMAT Null;
SELECT count(moduloOrZero(v, 65536)) FROM test_variant_oom2 FORMAT Null;
SELECT count(abs(v)) FROM test_variant_oom3 FORMAT Null;

DROP TABLE test_variant_oom;
DROP TABLE test_variant_oom2;
DROP TABLE test_variant_oom3;
