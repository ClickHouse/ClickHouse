-- Tags: no-old-analyzer

-- The limits on the size of an output block of the block nested loop join. `max_joined_block_size_rows`
-- caps the rows of a block, `max_joined_block_size_bytes` caps their estimated size; the pairs a tile
-- produces are cut to whichever is reached first, and the rest is emitted in the following blocks.

SET join_algorithm = 'direct,parallel_hash,hash';
SET cross_to_inner_join_rewrite = 0;
SET max_block_size = 100000;
SET enable_lazy_columns_replication = 0;
SET max_threads = 1;

DROP TABLE IF EXISTS bnl_bs_probe;
DROP TABLE IF EXISTS bnl_bs_build;

CREATE TABLE bnl_bs_probe (x UInt64, s String) ENGINE = MergeTree ORDER BY x;
CREATE TABLE bnl_bs_build (y UInt64) ENGINE = MergeTree ORDER BY y;
INSERT INTO bnl_bs_probe SELECT number, repeat('a', 1000) FROM numbers(500);
INSERT INTO bnl_bs_build SELECT number FROM numbers(500);

-- Neither limit set: the whole result of a probe chunk goes out in as few blocks as `max_block_size`
-- allows, so the blocks are far larger than either limit below would let them be.
SELECT 'unlimited', max(bs) > 10000
FROM (SELECT blockSize() AS bs FROM (
    SELECT l.s, r.y FROM bnl_bs_probe l LEFT JOIN bnl_bs_build r ON l.x < r.y))
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 0;

SELECT 'rows', max(bs)
FROM (SELECT blockSize() AS bs FROM (
    SELECT l.s, r.y FROM bnl_bs_probe l LEFT JOIN bnl_bs_build r ON l.x < r.y))
SETTINGS max_joined_block_size_rows = 111, max_joined_block_size_bytes = 0;

-- A row is about a kilobyte wide, so a budget of 10 KB is reached long before 100000 rows are.
SELECT 'bytes', max(bs) < 1000
FROM (SELECT blockSize() AS bs FROM (
    SELECT l.s, r.y FROM bnl_bs_probe l LEFT JOIN bnl_bs_build r ON l.x < r.y))
SETTINGS max_joined_block_size_rows = 100000, max_joined_block_size_bytes = 10000;

-- Whichever limit is reached first wins, and neither of them changes the result.
SELECT 'result', count(), sum(r.y)
FROM bnl_bs_probe l LEFT JOIN bnl_bs_build r ON l.x < r.y
SETTINGS max_joined_block_size_rows = 100000, max_joined_block_size_bytes = 10000;
SELECT 'result', count(), sum(r.y)
FROM bnl_bs_probe l LEFT JOIN bnl_bs_build r ON l.x < r.y
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 0;

DROP TABLE bnl_bs_probe;
DROP TABLE bnl_bs_build;
