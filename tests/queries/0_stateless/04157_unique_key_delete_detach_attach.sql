-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- UNIQUE KEY synchronous DELETE survives DETACH/ATTACH: the manifest + delete
-- bitmaps must persist and reload, so a delete's effect is identical before and
-- after a table reload. End-to-end backstop for the IDataPartStorage bitmap
-- persistence and the lazy-load metadata path.
-- All keys distinct (dedup is a later PR); merges stopped.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS uk_reload;

CREATE TABLE uk_reload (id UInt64, p UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
PARTITION BY p
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES uk_reload;

INSERT INTO uk_reload VALUES
    (1, 10, 'a'), (2, 10, 'b'), (3, 10, 'c'),
    (4, 20, 'd'), (5, 20, 'e'), (6, 20, 'f');

-- Delete across both partitions before the reload.
DELETE FROM uk_reload WHERE id IN (2, 5);

SELECT 'before_count' AS step, count() FROM uk_reload;  -- 4
SELECT 'before_survivors' AS step, id, p, v FROM uk_reload ORDER BY id;  -- 1,3,4,6

-- Reload: the in-memory manifest/bitmaps are dropped and rebuilt from disk.
DETACH TABLE uk_reload;
ATTACH TABLE uk_reload;

-- Merges were stopped before detach; re-stop after attach to be safe (the
-- survivor set must not depend on background activity either way).
SYSTEM STOP MERGES uk_reload;

-- Identical survivor set after reload: the persisted bitmaps were reloaded.
SELECT 'after_count' AS step, count() FROM uk_reload;  -- 4
SELECT 'after_survivors' AS step, id, p, v FROM uk_reload ORDER BY id;  -- 1,3,4,6
-- Partition-predicate counts also reconcile against the reloaded bitmaps.
SELECT 'after_p10' AS step, count() FROM uk_reload WHERE p = 10;  -- 1,3 -> 2
SELECT 'after_p20' AS step, count() FROM uk_reload WHERE p = 20;  -- 4,6 -> 2

-- A further DELETE after reload still works (manifest is writable post-attach).
DELETE FROM uk_reload WHERE id = 3;
SELECT 'after_more_delete' AS step, count() FROM uk_reload;  -- 3
SELECT 'after_more_survivors' AS step, id FROM uk_reload ORDER BY id;  -- 1,4,6

DROP TABLE uk_reload;
