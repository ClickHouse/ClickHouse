-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- UNIQUE KEY synchronous DELETE: multi-partition DELETE, partition-predicate
-- count() over deleted data (totalRowsByPartitionPredicate with deletes),
-- granule-skip end-to-end (a fully-dead granule is skipped by
-- selectLiveMarkRanges), and edge / cumulative predicates.
-- All keys are distinct (intra-block / cross-part dedup is a later PR), merges
-- are stopped (merge-side bitmap forwarding is a later PR).

SET allow_experimental_unique_key = 1;
SET async_insert = 0;
-- count() must consult the delete bitmap, not the rows_count shortcut or an
-- implicit projection (both ignore the per-part delete bitmap).
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

-- ============================================================================
-- 1. Multi-partition DELETE + partition-predicate count() after delete.
-- ============================================================================
DROP TABLE IF EXISTS uk_part_del;

CREATE TABLE uk_part_del (id UInt64, p UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
PARTITION BY p;

SYSTEM STOP MERGES uk_part_del;

-- 3 partitions (p = 10, 20, 30), 3 distinct rows each.
INSERT INTO uk_part_del VALUES
    (1, 10, 'a'), (2, 10, 'b'), (3, 10, 'c'),
    (4, 20, 'd'), (5, 20, 'e'), (6, 20, 'f'),
    (7, 30, 'g'), (8, 30, 'h'), (9, 30, 'i');

SELECT 'part_after_insert' AS step, count() FROM uk_part_del;  -- 9

-- DELETE spanning all partitions (every even id): 2,4,6,8 -> 4 rows gone.
DELETE FROM uk_part_del WHERE id % 2 = 0;

SELECT 'part_after_delete' AS step, count() FROM uk_part_del;  -- 5
-- Partition-predicate count() must reconcile the delete bitmap per partition.
SELECT 'part_p10' AS step, count() FROM uk_part_del WHERE p = 10;  -- 1,3 -> 2
SELECT 'part_p20' AS step, count() FROM uk_part_del WHERE p = 20;  -- 5 -> 1
SELECT 'part_p30' AS step, count() FROM uk_part_del WHERE p = 30;  -- 7,9 -> 2
SELECT 'part_survivors' AS step, id, p FROM uk_part_del ORDER BY id;  -- 1,3,5,7,9

DROP TABLE uk_part_del;

-- ============================================================================
-- 2. Granule-skip end-to-end: a whole granule goes fully dead, so
--    selectLiveMarkRanges skips that mark range. index_granularity = 4 with 8
--    distinct rows -> 2 granules (rows 0..3 and 4..7). Deleting all 4 keys in
--    the first granule makes mark 0 fully dead.
-- ============================================================================
DROP TABLE IF EXISTS uk_granule;

CREATE TABLE uk_granule (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES uk_granule;

INSERT INTO uk_granule VALUES
    (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'),
    (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h');

SELECT 'gran_after_insert' AS step, count() FROM uk_granule;  -- 8

-- Delete the entire first granule (ids 1..4 -> rows 0..3 fully dead).
DELETE FROM uk_granule WHERE id <= 4;

SELECT 'gran_after_delete' AS step, count() FROM uk_granule;  -- 4
SELECT 'gran_survivors' AS step, id, v FROM uk_granule ORDER BY id;  -- 5..8
-- A bounded read that would otherwise touch the dead granule still returns only
-- live survivors.
SELECT 'gran_filtered' AS step, count() FROM uk_granule WHERE id < 6;  -- only 5

DROP TABLE uk_granule;

-- ============================================================================
-- 3. Edge predicates: DELETE matching nothing (count unchanged) and DELETE all
--    rows in a part (erase_if empties it -> count 0).
-- ============================================================================
DROP TABLE IF EXISTS uk_edge;

CREATE TABLE uk_edge (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

SYSTEM STOP MERGES uk_edge;

INSERT INTO uk_edge VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');

-- Matches nothing: count unchanged, no rows marked dead.
DELETE FROM uk_edge WHERE id > 1000;
SELECT 'edge_nomatch' AS step, count() FROM uk_edge;  -- 4

-- Matches all rows in the part: empties it.
DELETE FROM uk_edge WHERE id >= 1;
SELECT 'edge_all' AS step, count() FROM uk_edge;  -- 0
SELECT 'edge_all_rows' AS step, id FROM uk_edge ORDER BY id;  -- (empty)

DROP TABLE uk_edge;

-- ============================================================================
-- 4. Cumulative DELETEs with DIFFERENT predicates on the same data: each adds
--    to the dead set; the survivor set is the intersection of all NOT-predicate.
-- ============================================================================
DROP TABLE IF EXISTS uk_cumulative;

CREATE TABLE uk_cumulative (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

SYSTEM STOP MERGES uk_cumulative;

INSERT INTO uk_cumulative VALUES
    (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'),
    (6, 'f'), (7, 'g'), (8, 'h'), (9, 'i'), (10, 'j');

SELECT 'cum_after_insert' AS step, count() FROM uk_cumulative;  -- 10

DELETE FROM uk_cumulative WHERE id <= 2;          -- kills 1,2
SELECT 'cum_step1' AS step, count() FROM uk_cumulative;  -- 8

DELETE FROM uk_cumulative WHERE v IN ('e', 'f');  -- kills 5,6 (disjoint)
SELECT 'cum_step2' AS step, count() FROM uk_cumulative;  -- 6

DELETE FROM uk_cumulative WHERE id >= 9;          -- kills 9,10
SELECT 'cum_step3' AS step, count() FROM uk_cumulative;  -- 4
SELECT 'cum_survivors' AS step, id, v FROM uk_cumulative ORDER BY id;  -- 3,4,7,8

DROP TABLE uk_cumulative;
