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

-- Each SELECT below pins max_threads = 1, so the peak is one Variant chunk instead of a multiple
-- of it, and max_untracked_memory = 0, so the limit is enforced at the allocation that crosses it
-- rather than at the next batched update. Without both pins the budget is crossed only when
-- several streams happen to be live at once, which is what let these assertions pass silently.
-- The budgets are chosen per path so that the throw happens inside the castColumn whose exception
-- filter the path exercises; a larger budget moves it to a later allocation outside that filter.
SELECT moduloOrZero(v, 65536) FROM test_variant_oom SETTINGS max_memory_usage = 2000000, max_threads = 1, max_untracked_memory = 0 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Path 2: Single variant type + NULLs → castColumn for Variant result with filter
DROP TABLE IF EXISTS test_variant_oom2;
CREATE TABLE test_variant_oom2 (v Variant(Float64, Int64)) ENGINE = Memory;
INSERT INTO test_variant_oom2 SELECT if(number % 3 = 0, NULL, (number * 1.0001)::Float64)::Variant(Float64, Int64) FROM numbers(5000000);

SELECT moduloOrZero(v, 65536) FROM test_variant_oom2 SETTINGS max_memory_usage = 2000000, max_threads = 1, max_untracked_memory = 0 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Path 3: Multiple variant types → general castColumn path. abs is used because abs(Int64) and
-- abs(UInt64) are both UInt64: the duplicate result type makes direct Variant construction
-- inapplicable, which is what selects the general path. moduloOrZero over two alternatives yields
-- two distinct result types and is built directly instead, without reaching any cast.
DROP TABLE IF EXISTS test_variant_oom3;
CREATE TABLE test_variant_oom3 (v Variant(Int64, UInt64, Float64)) ENGINE = Memory;
INSERT INTO test_variant_oom3 SELECT multiIf(number % 3 = 0, (-number)::Int64::Variant(Int64, UInt64, Float64), number % 3 = 1, number::UInt64::Variant(Int64, UInt64, Float64), (number * 1.5)::Float64::Variant(Int64, UInt64, Float64)) FROM numbers(5000000);

SELECT abs(v) FROM test_variant_oom3 SETTINGS max_memory_usage = 2000000, max_threads = 1, max_untracked_memory = 0 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Verify normal operation works fine
SELECT count(moduloOrZero(v, 65536)) FROM test_variant_oom FORMAT Null;
SELECT count(moduloOrZero(v, 65536)) FROM test_variant_oom2 FORMAT Null;
SELECT count(abs(v)) FROM test_variant_oom3 FORMAT Null;

DROP TABLE test_variant_oom;
DROP TABLE test_variant_oom2;
DROP TABLE test_variant_oom3;
