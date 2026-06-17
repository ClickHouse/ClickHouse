-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- UNIQUE KEY: merges are disabled (interim guard).
--
-- A merge would produce an output part without a `unique_key_index.sst` and
-- drop the input parts' delete bitmaps, so the merge-selection path skips
-- UNIQUE KEY tables entirely. Both background merges and explicit OPTIMIZE
-- must leave the parts uncoalesced.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET optimize_throw_if_noop = 0;

DROP TABLE IF EXISTS uk_no_merge;

CREATE TABLE uk_no_merge (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

-- Three distinct INSERTs => three active parts (each INSERT is its own part).
INSERT INTO uk_no_merge VALUES (1, 'a'), (2, 'b');
INSERT INTO uk_no_merge VALUES (3, 'c'), (4, 'd');
INSERT INTO uk_no_merge VALUES (5, 'e'), (6, 'f');

SELECT 'parts_after_insert' AS step, count() AS active_parts
FROM system.parts WHERE database = currentDatabase() AND table = 'uk_no_merge' AND active;

-- Explicit OPTIMIZE FINAL must be a no-op for a UNIQUE KEY table: merges are
-- disabled, so the part count is unchanged.
OPTIMIZE TABLE uk_no_merge FINAL;

SELECT 'parts_after_optimize' AS step, count() AS active_parts
FROM system.parts WHERE database = currentDatabase() AND table = 'uk_no_merge' AND active;

-- Data is fully readable regardless of part fan-out.
SELECT 'row_count' AS step, count() AS rows FROM uk_no_merge;
SELECT 'rows' AS step, id, v FROM uk_no_merge ORDER BY id;

DROP TABLE uk_no_merge;
