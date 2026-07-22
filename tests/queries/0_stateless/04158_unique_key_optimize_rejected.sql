-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- Merges are disabled for UNIQUE KEY tables (interim, until merge-side bitmap
-- forwarding + late-kill lands), so an explicit OPTIMIZE is rejected with
-- SUPPORT_IS_DISABLED. Background merges are gated at the same chokepoint;
-- INSERT / SELECT keep working.
-- All keys distinct (dedup is a later PR).

SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;
SET optimize_throw_if_noop = 1;

DROP TABLE IF EXISTS uk_optimize;

CREATE TABLE uk_optimize (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO uk_optimize VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO uk_optimize VALUES (4, 'd'), (5, 'e'), (6, 'f');

-- Two parts present; OPTIMIZE (and OPTIMIZE FINAL) must be rejected, not merge them.
OPTIMIZE TABLE uk_optimize; -- { serverError SUPPORT_IS_DISABLED }
OPTIMIZE TABLE uk_optimize FINAL; -- { serverError SUPPORT_IS_DISABLED }
-- DRY RUN PARTS bypasses StorageMergeTree::optimize and runs a real merge task;
-- it must be rejected at the MergeTreeData chokepoint as well.
OPTIMIZE TABLE uk_optimize DRY RUN PARTS 'all_1_1_0', 'all_2_2_0'; -- { serverError SUPPORT_IS_DISABLED }

-- Parts were not compacted: still two parts.
SELECT 'parts' AS step, count() FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_optimize' AND active;  -- 2

DROP TABLE uk_optimize;
