-- Regression for https://github.com/ClickHouse/ClickHouse/pull/110630:
-- SingleValueDataString::read must not preallocate the untrusted declared size upfront.
-- A valid AggregateFunction(max, String) state larger than a single compressed block is
-- read back through the incremental staging path, spanning many read-buffer refills.

DROP TABLE IF EXISTS t_single_value_multi_refill;
CREATE TABLE t_single_value_multi_refill (x AggregateFunction(max, String)) ENGINE = MergeTree ORDER BY tuple();

-- Small compressed blocks guarantee that the 8 MB state spans many read-buffer refills on
-- read, independent of the default buffer sizes, so the read goes through the incremental
-- `while (bytes_read < bytes_to_read)` staging loop rather than a single contiguous read.
INSERT INTO t_single_value_multi_refill
SELECT maxState(materialize(repeat('clickhouse', 800000)))
SETTINGS min_compress_block_size = 65536, max_compress_block_size = 65536;

-- The state round-trips byte-for-byte through the incremental read path.
SELECT length(maxMerge(x)) = 8000000, maxMerge(x) = repeat('clickhouse', 800000) FROM t_single_value_multi_refill;

-- Reading the same valid large state under a memory limit far below the payload is refused
-- with MEMORY_LIMIT_EXCEEDED: the staging buffer is StringWithMemoryTracking, which enforces
-- the limit as it grows (a plain String would only count the staged bytes, not refuse them).
SELECT maxMerge(x) FROM t_single_value_multi_refill SETTINGS max_memory_usage = '4Mi', max_untracked_memory = '1Mi' FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

DROP TABLE t_single_value_multi_refill;
