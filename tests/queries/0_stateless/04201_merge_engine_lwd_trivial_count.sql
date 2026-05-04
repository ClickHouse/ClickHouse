-- Test: count() over `Merge` engine wrapping a `MergeTree` table with lightweight deletes
-- must NOT use the trivial count optimization, and must return the correct post-delete count.
--
-- Code path: `StorageMerge::supportsTrivialCountOptimization` (src/Storages/StorageMerge.cpp)
-- delegates to inner table's `MergeTreeData::supportsTrivialCountOptimization(nullptr, ctx)`.
-- The nullptr-snapshot fallback in MergeTreeData (src/Storages/MergeTree/MergeTreeData.cpp)
-- checks `has_lightweight_delete_parts` and returns false if any inner part has an LWD mask.
-- This disables trivial count for the wrapping `Merge` table -> count() reads actual data.
--
-- Risk if broken: count() over `Merge(...)` returns stale part-level row counts after
-- lightweight DELETE on inner tables -> users get wrong row counts silently.
--
-- Existing coverage: tests/queries/0_stateless/04077_lwd_trivial_count.sql covers
-- the same path for direct MergeTree (not via Merge engine). 02918_optimize_count_for_merge_tables.sql
-- covers Merge over MergeTree without lightweight delete. The combination is uncovered.

DROP TABLE IF EXISTS t_lwd_inner;
DROP TABLE IF EXISTS t_lwd_merge;

CREATE TABLE t_lwd_inner (a UInt32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_lwd_merge (a UInt32) ENGINE = Merge(currentDatabase(), '^t_lwd_inner$');

INSERT INTO t_lwd_inner SELECT number FROM numbers(1000);

SET apply_mutations_on_fly = 0;
SET apply_patch_parts = 0;
SET optimize_trivial_count_query = 1;
SET lightweight_deletes_sync = 2;

-- Before any LWD: trivial count IS allowed for Merge over MergeTree.
SELECT 'before_lwd_count', count() FROM t_lwd_merge;
SELECT trimBoth(explain) FROM (EXPLAIN SELECT count() FROM t_lwd_merge SETTINGS enable_analyzer=1)
WHERE explain LIKE '%Optimized trivial count%' OR explain LIKE '%ReadFromMerge%';

-- Apply lightweight DELETE on the inner MergeTree
DELETE FROM t_lwd_inner WHERE a < 100;

-- After LWD on inner table: trivial count must be DISABLED for the Merge table.
-- count() must return 900, NOT the stale 1000 from part metadata.
SELECT 'after_lwd_count', count() FROM t_lwd_merge;
SELECT trimBoth(explain) FROM (EXPLAIN SELECT count() FROM t_lwd_merge SETTINGS enable_analyzer=1)
WHERE explain LIKE '%Optimized trivial count%' OR explain LIKE '%ReadFromMerge%';

DROP TABLE t_lwd_merge;
DROP TABLE t_lwd_inner;
