-- Tags: no-old-analyzer

-- `max_joined_block_size_bytes` bounds every output block of the block nested loop join, including
-- the two that are padded rather than matched: the probe rows a `LEFT` join keeps and the build rows
-- a `RIGHT` join keeps. Both used to be cut by row count alone, so a wide row made them overshoot
-- the byte budget by orders of magnitude. The budget counts the side that carries data; what the
-- padded columns add is left out of the estimate, since for most types their defaults cost nothing.

SET enable_analyzer = 1;
SET cross_to_inner_join_rewrite = 0;
SET max_threads = 1;
SET max_block_size = 100000;
SET enable_lazy_columns_replication = 0;

DROP TABLE IF EXISTS bnl_pad_probe;
DROP TABLE IF EXISTS bnl_pad_build;

CREATE TABLE bnl_pad_probe (x UInt64, s String) ENGINE = MergeTree ORDER BY x;
CREATE TABLE bnl_pad_build (y UInt64, t String) ENGINE = MergeTree ORDER BY y;
INSERT INTO bnl_pad_probe SELECT number, repeat('a', 1000) FROM numbers(500);
INSERT INTO bnl_pad_build SELECT number, repeat('b', 1000) FROM numbers(500);

-- Nothing matches, so the whole result of each of these is the padded side.
SELECT 'unmatched probe rows, unlimited', max(bs)
FROM (SELECT blockSize() AS bs FROM (
    SELECT l.s FROM bnl_pad_probe l LEFT JOIN bnl_pad_build r ON l.x + 1000 < r.y))
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 0;

-- A row is about a kilobyte wide, so a 10 KB budget holds about ten of them.
SELECT 'unmatched probe rows, bytes', max(bs) < 100
FROM (SELECT blockSize() AS bs FROM (
    SELECT l.s FROM bnl_pad_probe l LEFT JOIN bnl_pad_build r ON l.x + 1000 < r.y))
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 10000;

SELECT 'unmatched build rows, unlimited', max(bs)
FROM (SELECT blockSize() AS bs FROM (
    SELECT r.t FROM bnl_pad_probe l RIGHT JOIN bnl_pad_build r ON l.x > r.y + 1000))
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 0;

SELECT 'unmatched build rows, bytes', max(bs) < 100
FROM (SELECT blockSize() AS bs FROM (
    SELECT r.t FROM bnl_pad_probe l RIGHT JOIN bnl_pad_build r ON l.x > r.y + 1000))
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 10000;

-- Neither limit changes the result.
SELECT 'result', count(), countIf(t = ''), countIf(s = '')
FROM (
    SELECT l.s AS s, r.t AS t FROM bnl_pad_probe l FULL JOIN bnl_pad_build r ON l.x + 1000 < r.y)
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 10000;

SELECT 'result', count(), countIf(t = ''), countIf(s = '')
FROM (
    SELECT l.s AS s, r.t AS t FROM bnl_pad_probe l FULL JOIN bnl_pad_build r ON l.x + 1000 < r.y)
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 0;

DROP TABLE bnl_pad_probe;
DROP TABLE bnl_pad_build;
