-- Regression test for ClickHouse/ClickHouse#109948:
-- `FractionalLimitTransform::makeChunkWithPreviousRow` asserts `row < chunk.getNumRows()` and was
-- called with `chunk_rows - 1` whenever `with_ties && rows_processed == offset_rows + limit_rows`,
-- without checking that the chunk has any rows. A trailing empty chunk from the `WITH FILL`
-- pipeline reaching the transform right at the limit boundary made the call `chunk_rows - 1`
-- underflow to `UINT64_MAX`, tripping the assertion (server abort in debug/sanitizer builds,
-- LOGICAL_ERROR in release). Found by the AST fuzzer.

-- max_block_size=1 forces the boundary chunk and the trailing empty chunk into separate chunks.
SELECT number FROM numbers(20) ORDER BY number ASC WITH FILL STEP 2 STALENESS 1 LIMIT 0.5, 0.5 WITH TIES
SETTINGS max_block_size = 1, max_threads = 1;
