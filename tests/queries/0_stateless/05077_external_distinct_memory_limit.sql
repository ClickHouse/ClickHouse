-- Tags: no-asan, no-flaky-check
-- Real memory accounting determines whether the hash set fits within `max_memory_usage`. ASan overhead
-- and concurrent flaky-check runs change that boundary, so this test excludes those configurations.
SET max_bytes_ratio_before_external_distinct = 0;

-- Preliminary `DISTINCT` keeps deduplicating mostly unique input when spilling is disabled, preserving
-- the memory pressure needed to distinguish the in-memory and external algorithms.
SELECT count() FROM (SELECT DISTINCT number % 8000000 AS k FROM numbers(16000000)) SETTINGS max_memory_usage = '120M', max_bytes_before_external_distinct = 0, allow_preliminary_distinct_abandoning = 0; -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT count() FROM (SELECT DISTINCT number % 8000000 AS k FROM numbers(16000000)) SETTINGS max_memory_usage = '120M', max_bytes_before_external_distinct = '30M', allow_preliminary_distinct_abandoning = 0;
