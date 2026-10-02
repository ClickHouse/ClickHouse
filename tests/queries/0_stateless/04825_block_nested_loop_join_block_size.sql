-- Tags: no-old-analyzer

-- The limits on the size of an output block of the block nested loop join. `max_joined_block_size_rows`
-- caps the rows of a block, `max_joined_block_size_bytes` caps their estimated size; the pairs a tile
-- produces are cut to whichever is reached first, and the rest is emitted in the following blocks.
--
-- Both limits bound every output block, the two that are padded rather than matched included: the
-- probe rows a `LEFT` join keeps and the build rows a `RIGHT` join keeps. Those two used to be cut by
-- row count alone, so a wide row made them overshoot the byte budget by orders of magnitude. The
-- budget counts the side that carries data; what the padded columns add is left out of the estimate,
-- since for most types their defaults cost nothing.

SET cross_to_inner_join_rewrite = 0;
SET max_threads = 1;
SET max_block_size = 100000;
SET enable_lazy_columns_replication = 0;

DROP TABLE IF EXISTS bnl_bs_probe;
DROP TABLE IF EXISTS bnl_bs_build;

CREATE TABLE bnl_bs_probe (x UInt64, s String) ENGINE = MergeTree ORDER BY x;
CREATE TABLE bnl_bs_build (y UInt64, t String) ENGINE = MergeTree ORDER BY y;
INSERT INTO bnl_bs_probe SELECT number, repeat('a', 1000) FROM numbers(500);
INSERT INTO bnl_bs_build SELECT number, repeat('b', 1000) FROM numbers(500);

-- Neither limit set: the whole result of a probe chunk goes out in as few blocks as `max_block_size`
-- allows, so the blocks are far larger than either limit below would let them be.
SELECT 'matched, unlimited', max(bs) > 10000
FROM (SELECT blockSize() AS bs FROM (
    SELECT l.s, r.y FROM bnl_bs_probe l LEFT JOIN bnl_bs_build r ON l.x < r.y))
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 0;

SELECT 'matched, rows', max(bs)
FROM (SELECT blockSize() AS bs FROM (
    SELECT l.s, r.y FROM bnl_bs_probe l LEFT JOIN bnl_bs_build r ON l.x < r.y))
SETTINGS max_joined_block_size_rows = 111, max_joined_block_size_bytes = 0;

-- A row is about a kilobyte wide, so a budget of 10 KB is reached long before 100000 rows are.
SELECT 'matched, bytes', max(bs) < 1000
FROM (SELECT blockSize() AS bs FROM (
    SELECT l.s, r.y FROM bnl_bs_probe l LEFT JOIN bnl_bs_build r ON l.x < r.y))
SETTINGS max_joined_block_size_rows = 100000, max_joined_block_size_bytes = 10000;

-- Nothing matches, so the whole result of each of these is the padded side.
SELECT 'unmatched probe rows, unlimited', max(bs)
FROM (SELECT blockSize() AS bs FROM (
    SELECT l.s FROM bnl_bs_probe l LEFT JOIN bnl_bs_build r ON l.x + 1000 < r.y))
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 0;

SELECT 'unmatched probe rows, bytes', max(bs) < 100
FROM (SELECT blockSize() AS bs FROM (
    SELECT l.s FROM bnl_bs_probe l LEFT JOIN bnl_bs_build r ON l.x + 1000 < r.y))
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 10000;

SELECT 'unmatched build rows, unlimited', max(bs)
FROM (SELECT blockSize() AS bs FROM (
    SELECT r.t FROM bnl_bs_probe l RIGHT JOIN bnl_bs_build r ON l.x > r.y + 1000))
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 0;

SELECT 'unmatched build rows, bytes', max(bs) < 100
FROM (SELECT blockSize() AS bs FROM (
    SELECT r.t FROM bnl_bs_probe l RIGHT JOIN bnl_bs_build r ON l.x > r.y + 1000))
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 10000;

-- Whichever limit is reached first wins, and neither of them changes the result, matched or padded.
SELECT 'matched result same',
    (SELECT (count(), sum(r.y)) FROM bnl_bs_probe l LEFT JOIN bnl_bs_build r ON l.x < r.y
     SETTINGS max_joined_block_size_rows = 100000, max_joined_block_size_bytes = 10000)
  = (SELECT (count(), sum(r.y)) FROM bnl_bs_probe l LEFT JOIN bnl_bs_build r ON l.x < r.y
     SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 0) AS ok;

SELECT 'padded result same',
    (SELECT (count(), countIf(t = ''), countIf(s = '')) FROM (
        SELECT l.s AS s, r.t AS t FROM bnl_bs_probe l FULL JOIN bnl_bs_build r ON l.x + 1000 < r.y)
     SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 10000)
  = (SELECT (count(), countIf(t = ''), countIf(s = '')) FROM (
        SELECT l.s AS s, r.t AS t FROM bnl_bs_probe l FULL JOIN bnl_bs_build r ON l.x + 1000 < r.y)
     SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 0) AS ok;

DROP TABLE bnl_bs_probe;
DROP TABLE bnl_bs_build;
