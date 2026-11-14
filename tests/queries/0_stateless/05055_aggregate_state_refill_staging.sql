-- Tags: long

-- An aggregate state value that arrives across compressed-block boundaries must round-trip
-- unchanged, and staging it must cost the value rather than the number of pieces it arrives in.
-- A `CAST(unhex(...))` blob is always fully buffered, so only a state read back from a table
-- reaches the staged path.

-- Part type and both block sizes are randomized per run, so all three are pinned: the state below
-- spans 31 blocks here, and a compact part ignores a block size this small and writes 2 of 1 MiB.
DROP TABLE IF EXISTS t_deserialize_allocation_bomb_refill;
CREATE TABLE t_deserialize_allocation_bomb_refill
(
    gua AggregateFunction(groupUniqArray, String)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
    min_compress_block_size = 65536, max_compress_block_size = 65536;

INSERT INTO t_deserialize_allocation_bomb_refill
SELECT (SELECT groupUniqArrayState(s) FROM (SELECT repeat('x', 1000000) AS s UNION ALL SELECT repeat('y', 1000000) AS s));

SELECT sum(length(x)), countIf(x = repeat('x', 1000000)) + countIf(x = repeat('y', 1000000)) FROM (SELECT arrayJoin(groupUniqArrayMerge(gua)) AS x FROM t_deserialize_allocation_bomb_refill);

DROP TABLE t_deserialize_allocation_bomb_refill;

-- Staging a value that arrives in pieces must cost the value, not the number of pieces: this one
-- arrives in 250001 blocks and the limit is five times what the read needs. The uncompressed cache
-- is pinned off because caching that many blocks would cost more than this assertion measures.
DROP TABLE IF EXISTS t_deserialize_allocation_bomb_chunks;
CREATE TABLE t_deserialize_allocation_bomb_chunks
(
    gua AggregateFunction(groupUniqArray, String)
)
ENGINE = MergeTree ORDER BY tuple()
-- Packed storage: a full-storage stream's file buffer is one compress block, so at this size
-- every block costs a write syscall.
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_full_part_storage = 2,
    min_compress_block_size = 4, max_compress_block_size = 4;

INSERT INTO t_deserialize_allocation_bomb_chunks
SELECT (SELECT groupUniqArrayState(s) FROM (SELECT repeat('z', 1000000) AS s));

SELECT sum(length(x)), countIf(x = repeat('z', 1000000)) FROM (SELECT arrayJoin(groupUniqArrayMerge(gua)) AS x FROM t_deserialize_allocation_bomb_chunks) SETTINGS max_memory_usage = 8000000, use_uncompressed_cache = 0;

DROP TABLE t_deserialize_allocation_bomb_chunks;

-- Both limbs of a statistics state arrive one element at a time here: at 4-byte blocks
-- `available` is below `sizeof(Float64)`, so every element spans a refill and the sample grows
-- once per element. A fully buffered blob reads all of it in a single batch instead.
DROP TABLE IF EXISTS t_deserialize_allocation_bomb_stat;
CREATE TABLE t_deserialize_allocation_bomb_stat
(
    mw AggregateFunction(mannWhitneyUTest, Float64, UInt8)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
    min_compress_block_size = 4, max_compress_block_size = 4;

INSERT INTO t_deserialize_allocation_bomb_stat
SELECT mannWhitneyUTestState(number::Float64, (number % 2)::UInt8) FROM numbers(100);

SELECT mannWhitneyUTestMerge(mw) FROM t_deserialize_allocation_bomb_stat;

DROP TABLE t_deserialize_allocation_bomb_stat;
