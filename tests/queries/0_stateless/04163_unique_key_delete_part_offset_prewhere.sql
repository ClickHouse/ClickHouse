-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage, no-fasttest
-- UNIQUE KEY synchronous DELETE: PREWHERE that REFERENCES `_part_offset` itself.
-- The read-path delete-bitmap filter keys on `_part_offset`; if PREWHERE consumes
-- and prunes that virtual column from the post-PREWHERE block, the filter would be
-- silently skipped and deleted rows would reappear. The fix keeps `_part_offset`
-- in the output for UK-with-bitmap reads, so dead rows stay hidden here.
-- All keys distinct (dedup is a later PR); merges stopped.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS uk_po_prewhere;

CREATE TABLE uk_po_prewhere (id UInt64, grp UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES uk_po_prewhere;

INSERT INTO uk_po_prewhere VALUES
    (1, 100, 'a'), (2, 100, 'b'), (3, 200, 'c'),
    (4, 200, 'd'), (5, 100, 'e'), (6, 200, 'f');
SELECT 'po_after_insert' AS step, count() FROM uk_po_prewhere;  -- 6

-- Kill two rows.
DELETE FROM uk_po_prewhere WHERE id IN (2, 4);
SELECT 'po_after_delete' AS step, count() FROM uk_po_prewhere;  -- 4

-- PREWHERE references `_part_offset` directly. Without the fix, `_part_offset`
-- is consumed by PREWHERE and pruned from the post-PREWHERE block, the delete
-- bitmap is skipped, and the dead rows (2, 4) reappear -> count() == 6 and the
-- survivor list includes b/d. With the fix the bitmap still applies -> count() == 4.
SELECT 'po_all_count' AS step, count() FROM uk_po_prewhere PREWHERE _part_offset >= 0;  -- 4
SELECT 'po_all_rows' AS step, id, v FROM uk_po_prewhere PREWHERE _part_offset >= 0 ORDER BY id;  -- 1 3 5 6

-- PREWHERE referencing `_part_offset` AND a physical column (multi-input predicate).
SELECT 'po_grp100' AS step, id, v FROM uk_po_prewhere PREWHERE _part_offset >= 0 AND grp = 100 ORDER BY id;  -- 1 a / 5 e

-- PREWHERE selecting `_part_offset` into the output as well (column survives + is returned).
SELECT 'po_with_offset' AS step, count() FROM uk_po_prewhere PREWHERE _part_offset < 1000;  -- 4

DROP TABLE uk_po_prewhere;
