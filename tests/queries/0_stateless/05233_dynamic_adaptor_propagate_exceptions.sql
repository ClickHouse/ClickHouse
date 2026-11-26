-- Test that non-type-conversion exceptions (like MEMORY_LIMIT_EXCEEDED) are propagated
-- from FunctionDynamicAdaptor instead of being incorrectly wrapped as LOGICAL_ERROR.
-- The Dynamic sibling of 04101_variant_adaptor_propagate_exceptions.

SET allow_suspicious_fixed_string_types = 1;

-- `upper` declares String as its Dynamic result type but returns FixedString(N) for a
-- FixedString(N) variant, so the adaptor must cast FixedString(N) -> String. A wide FixedString
-- makes that cast the query's peak allocation, so a limit inside the verified window below is
-- crossed by the cast itself and not by an earlier step.

-- Path 1: single variant, no NULLs. Verified window for this fixture: [70MB, 200MB]; below it the
-- limit is crossed before the cast (vacuous), above it the query succeeds.
DROP TABLE IF EXISTS test_dynamic_oom;
CREATE TABLE test_dynamic_oom (d Dynamic) ENGINE = Memory;
INSERT INTO test_dynamic_oom SELECT (toString(number))::FixedString(1048577)::Dynamic FROM numbers(64) SETTINGS max_memory_usage = 0;

SELECT upper(d) FROM test_dynamic_oom SETTINGS max_memory_usage = 120000000, max_threads = 1, max_untracked_memory = 0 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Path 2: single variant + NULLs. The result is expanded back to full size before the cast, so the
-- window sits higher. Verified window for this fixture: [150MB, 260MB].
DROP TABLE IF EXISTS test_dynamic_oom2;
CREATE TABLE test_dynamic_oom2 (d Dynamic) ENGINE = Memory;
INSERT INTO test_dynamic_oom2 SELECT if(number % 8 = 0, (toString(number))::FixedString(1048577), NULL)::Dynamic FROM numbers(96) SETTINGS max_memory_usage = 0;

SELECT upper(d) FROM test_dynamic_oom2 SETTINGS max_memory_usage = 200000000, max_threads = 1, max_untracked_memory = 0 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Path 3: multiple variants, cast once per variant. Two FixedString widths keep both variants
-- non-empty and both mismatching. Verified window for this fixture: [80MB, 220MB].
DROP TABLE IF EXISTS test_dynamic_oom3;
CREATE TABLE test_dynamic_oom3 (d Dynamic) ENGINE = Memory;
INSERT INTO test_dynamic_oom3 SELECT multiIf(number % 2 = 0, (toString(number))::FixedString(1048577)::Dynamic, (toString(number))::FixedString(524288)::Dynamic) FROM numbers(128) SETTINGS max_memory_usage = 0;

SELECT upper(d) FROM test_dynamic_oom3 SETTINGS max_memory_usage = 130000000, max_threads = 1, max_untracked_memory = 0 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Each fixture must stay in a single block: a split would shrink the per-block cast allocation and
-- silently move the throw out of the cast, which is what would make the assertions above stop
-- asserting anything.
SELECT count() FROM (SELECT 1 FROM test_dynamic_oom GROUP BY blockNumber());
SELECT count() FROM (SELECT 1 FROM test_dynamic_oom2 GROUP BY blockNumber());
SELECT count() FROM (SELECT 1 FROM test_dynamic_oom3 GROUP BY blockNumber());

-- Verify normal operation works fine, and pin the non-NULL count of each fixture.
SELECT count(upper(d)) FROM test_dynamic_oom;
SELECT count(upper(d)) FROM test_dynamic_oom2;
SELECT count(upper(d)) FROM test_dynamic_oom3;

DROP TABLE test_dynamic_oom;
DROP TABLE test_dynamic_oom2;
DROP TABLE test_dynamic_oom3;
