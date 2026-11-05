-- Tags: no-async-insert
-- Regression test for byte underestimation in PendingQueue::consumeUpTo.
--
-- Root cause: calculateConsumable uses a floating-point round-trip
--   bytes_to_take = size_t(rows_to_take * (total_bytes / total_rows))
-- which can be M-1 when M % N != 0 (IEEE-754 truncation).
--
-- This causes pending.total_bytes (tracked with actual chunk bytes via pushBack)
-- to drift upward vs accumulated.bytes (built from estimated values), so
-- canGenerate() can return true while generate() returns {} => LOGICAL_ERROR.
--
-- Trigger conditions (N=7 rows per chunk):
--   Each chunk: 2 empty strings + 5 single-char 'a' strings.
--   ColumnString::byteSize() = chars + offsets (no null terminators):
--     chars   = 0+0+1+1+1+1+1 = 5 bytes
--     offsets = 7 * 8          = 56 bytes
--     total   = 61 bytes  (not divisible by 7)
--   bytes_per_row = 61.0/7.0 = 8.714285...
--   bytes_to_take = size_t(7 * 8.714285...) = 60  <- loses 1 byte per chunk
--
--   1000 chunks x 7 rows = 7000 rows
--   actual total bytes    = 1000 * 61 = 61000
--   estimated total bytes = 1000 * 60 = 60000
--
--   Setting min_insert_block_size_bytes = 60001 sits between the two:
--     canGenerate() sees 61000 >= 60001 -> true
--     generate()  accumulates only 60000 -> allMinReached() = false
--     -> returns {}  -> LOGICAL_ERROR in PlanSquashingTransform::onGenerate

DROP TABLE IF EXISTS test_byte_underestimation;

CREATE TABLE test_byte_underestimation (s String) ENGINE = MergeTree() ORDER BY s;

-- With max_block_size=7 and numbers starting at 0, chunk k contains numbers
-- {7k, ..., 7k+6}. number%7 is in {0,1} for the first two rows -> ''
-- and in {2,...,6} for the rest -> 'a'. Every chunk has exactly 2 empty
-- and 5 non-empty strings, giving byteSize = 61 (not divisible by 7).
INSERT INTO test_byte_underestimation
SELECT if(number % 7 < 2, '', 'a') FROM numbers(7000)
SETTINGS
    max_block_size = 7,
    min_insert_block_size_rows = 7000,
    min_insert_block_size_bytes = 60001,
    max_insert_block_size_bytes = 0,
    use_strict_insert_block_limits = 1;

SELECT count() FROM test_byte_underestimation;

DROP TABLE test_byte_underestimation;
