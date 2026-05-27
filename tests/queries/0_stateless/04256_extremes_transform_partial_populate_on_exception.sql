-- Regression for ExtremesTransform partial-population abort (STID 2286-2cea).
-- Pre-fix, `extremes_columns` was resized to N null entries then filled
-- column-by-column. A throw mid-loop left the vector partially populated;
-- the next `work()` call ran `Chunk::setColumns` on the broken vector and the
-- row-count diagnostic dereferenced one of the still-null intrusive pointers
-- via `Chunk::dumpStructure`, aborting the server with
-- `Assertion 'px != 0' failed`. The fix builds into a local vector and moves
-- it on success, so a mid-loop throw leaves `extremes_columns` empty.
--
-- The exact memory budget that triggers the throw inside the partial-population
-- loop varies by build (debug/release/sanitizer), so run several variants:
-- at least one will hit the bug path on each environment.

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
