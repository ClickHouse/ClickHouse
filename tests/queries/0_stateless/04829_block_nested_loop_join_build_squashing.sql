-- Tags: no-old-analyzer

-- The build side of a block nested loop join is squashed to `min_joined_block_size_rows` /
-- `min_joined_block_size_bytes` before it is stored, the way every other join squashes its right
-- input. It matters more here: a tile of candidate pairs never spans two stored blocks, so a right
-- input written in small blocks would otherwise cost one evaluation of the condition per block per
-- probe chunk, and a stage that walks the store would emit one chunk per block.

SET enable_analyzer = 1;
SET join_algorithm = 'partial_merge';
SET query_plan_join_swap_table = 'false';
SET max_threads = 1;

DROP TABLE IF EXISTS bnl_squash_probe;
DROP TABLE IF EXISTS bnl_squash_build;

CREATE TABLE bnl_squash_probe (x UInt64) ENGINE = Memory;
CREATE TABLE bnl_squash_build (y UInt64) ENGINE = Memory;

INSERT INTO bnl_squash_probe SELECT number FROM numbers(4);
-- One block per row, which is what a table filled by many small inserts looks like.
INSERT INTO bnl_squash_build SELECT number FROM numbers(300) SETTINGS max_block_size = 1, max_insert_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

SELECT 'squashed', count() FROM (
    EXPLAIN PIPELINE SELECT * FROM bnl_squash_probe l JOIN bnl_squash_build r ON l.x < r.y)
WHERE explain LIKE '%SimpleSquashingTransform%';

SELECT 'not squashed', count() FROM (
    EXPLAIN PIPELINE SELECT * FROM bnl_squash_probe l JOIN bnl_squash_build r ON l.x < r.y
    SETTINGS min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0)
WHERE explain LIKE '%SimpleSquashingTransform%';

-- The block boundaries of the right input change nothing about the result, for the kinds that read
-- the store from the probe side and for the ones that scan it afterwards.
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
