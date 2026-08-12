-- Tags: no-old-analyzer

-- How the build side of a block nested loop join is cut into blocks changes nothing about the
-- result. The store keeps the blocks it is given, and a tile of candidate pairs never spans two of
-- them, so a right input written in small blocks costs one evaluation of the condition per block per
-- probe chunk and makes the stage that walks the store emit one chunk per block. That is what the
-- squashing to `min_joined_block_size_rows` / `min_joined_block_size_bytes` every join applies to its
-- right input is worth here; turning it off is how these queries produce a finely blocked store.

SET enable_analyzer = 1;
-- No algorithm is enabled, so every kind below is answered by the operator, `INNER` included.
SET join_algorithm = '';
SET query_plan_join_swap_table = 'false';
SET max_threads = 1;

DROP TABLE IF EXISTS bnl_squash_probe;
DROP TABLE IF EXISTS bnl_squash_build;

CREATE TABLE bnl_squash_probe (x UInt64) ENGINE = Memory;
CREATE TABLE bnl_squash_build (y UInt64) ENGINE = Memory;

INSERT INTO bnl_squash_probe SELECT number FROM numbers(4);
-- One block per row, which is what a table filled by many small inserts looks like.
INSERT INTO bnl_squash_build SELECT number FROM numbers(300) SETTINGS max_block_size = 1, max_insert_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

-- The kinds that read the store from the probe side and the ones that scan it afterwards, over a
-- store of 300 one-row blocks and over the same rows squashed into one.
SELECT 'inner', count(), sum(x * 1000 + y) FROM bnl_squash_probe l JOIN bnl_squash_build r ON l.x < r.y;
SELECT 'inner', count(), sum(x * 1000 + y) FROM bnl_squash_probe l JOIN bnl_squash_build r ON l.x < r.y
SETTINGS min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;

SELECT 'full', count(), sum(x * 1000 + y) FROM bnl_squash_probe l FULL JOIN bnl_squash_build r ON l.x + 1000 < r.y;
SELECT 'full', count(), sum(x * 1000 + y) FROM bnl_squash_probe l FULL JOIN bnl_squash_build r ON l.x + 1000 < r.y
SETTINGS min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;

-- The stage that emits the build rows nothing matched works one stored block at a time, so with the
-- right input squashed its chunks are the store's blocks and not the 300 blocks it was written in.
SELECT 'unmatched chunk rows', max(bs) FROM (
    SELECT blockSize() AS bs FROM bnl_squash_probe l RIGHT JOIN bnl_squash_build r ON l.x + 1000 < r.y);
SELECT 'unmatched chunk rows', max(bs) FROM (
    SELECT blockSize() AS bs FROM bnl_squash_probe l RIGHT JOIN bnl_squash_build r ON l.x + 1000 < r.y
    SETTINGS min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0);

-- A columnless build side, which the store cannot spill, squashes to a bare row count.
SELECT 'columnless', count() FROM bnl_squash_probe l JOIN bnl_squash_build r ON l.x < 1000;

DROP TABLE bnl_squash_probe;
DROP TABLE bnl_squash_build;
