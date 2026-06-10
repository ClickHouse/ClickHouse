-- Tags: no-fasttest
-- Needs rapidjson library

-- The output buffer of prettyPrintJSON is accounted in the query MemoryTracker,
-- so deeply nested input with a non-zero indent (O(N^2) output) throws
-- MEMORY_LIMIT_EXCEEDED instead of letting the process be OOM-killed. Output
-- here is ~8.6 MiB from a ~9 KB input, well above the 4 MB budget, so the
-- rejection is deterministic and cheap under parallel/sanitizer runs.
SELECT length(prettyPrintJSON(concat(repeat('{"a":', 1500), '1', repeat('}', 1500)), 4))
SETTINGS max_memory_usage = 4000000; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- A moderately nested document under the default (unlimited) budget succeeds.
SELECT length(prettyPrintJSON(concat(repeat('{"a":', 1000), '1', repeat('}', 1000)))) > 0;

-- The same input with indent = 0 stays O(N) and succeeds under a small budget.
SELECT length(prettyPrintJSON(concat(repeat('{"a":', 1000), '1', repeat('}', 1000)), 0)) > 0
SETTINGS max_memory_usage = 4000000;
