-- Regression for the ExtremesTransform partial-population abort (STID 2286-2cea).
-- Each variant uses a different memory budget so that at least one trips the
-- mid-population throw on every build flavor (debug/release/sanitizer); the
-- expected outcome is a clean MEMORY_LIMIT_EXCEEDED, not a server abort.

SELECT DISTINCT 1025, toFixedString('%', 1048576), 100 UNION ALL
SELECT 9223372036854775807, '\0', materialize(toLowCardinality(1025))
SETTINGS extremes = 1, max_memory_usage = '4Mi'
FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

SELECT DISTINCT 1025, toFixedString('%', 1572864), 100 UNION ALL
SELECT 9223372036854775807, '\0', materialize(toLowCardinality(1025))
SETTINGS extremes = 1, max_memory_usage = '6Mi'
FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

SELECT DISTINCT 1025, toFixedString('%', 2097152), 100 UNION ALL
SELECT 9223372036854775807, '\0', materialize(toLowCardinality(1025))
SETTINGS extremes = 1, max_memory_usage = '8Mi'
FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

SELECT DISTINCT 1025, toFixedString('%', 2621440), 100 UNION ALL
SELECT 9223372036854775807, '\0', materialize(toLowCardinality(1025))
SETTINGS extremes = 1, max_memory_usage = '10Mi'
FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

SELECT 'alive';
