-- Regression test for `Assertion 'px != 0' failed` in `ExtremesTransform::work` (STID: 2286-2cea).
--
-- Before the fix, `ExtremesTransform::transform` resized `extremes_columns` to N null
-- entries and filled them column-by-column. If `insert` of a large value raised
-- `MEMORY_LIMIT_EXCEEDED` halfway through the loop, the member was left partially
-- populated; the subsequent `ExtremesTransform::work` call invoked `Chunk::setColumns`
-- on the broken vector and the row-count error formatter dereferenced one of the
-- still-null intrusive pointers via `Chunk::dumpStructure`, aborting the server.
--
-- The fix builds into a local vector first and moves on success, so a throw during
-- column construction leaves `extremes_columns` empty and the server returns a normal
-- `MEMORY_LIMIT_EXCEEDED` exception to the client.
--
-- Reference CI report:
-- https://s3.amazonaws.com/clickhouse-test-reports/PRs/104000/66e1bac24c924afddb3c58be48a8174b37f0f2e3/ast_fuzzer_amd_debug/
-- PR: https://github.com/ClickHouse/ClickHouse/pull/104000

-- The query must trigger `MEMORY_LIMIT_EXCEEDED` while `ExtremesTransform::transform`
-- is in the middle of populating extremes columns. `toFixedString('%', 1048576)`
-- produces a 1 MiB value; copying it twice (min + max) into the extremes column under
-- a 4 MiB memory budget pushes the query past the limit.
--
-- `FORMAT Null` is used so the pipeline runs end-to-end (including the extremes branch)
-- without polluting the test output with the 1 MiB FixedString payload.
SELECT DISTINCT 1025, toFixedString('%', 1048576), 100
UNION ALL
SELECT 9223372036854775807, '\0', materialize(toLowCardinality(1025))
SETTINGS extremes = 1, max_memory_usage = '4Mi'
FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- The server must still be alive after the failed query above.
SELECT 'alive';
