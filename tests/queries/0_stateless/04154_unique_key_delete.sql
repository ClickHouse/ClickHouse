-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- UNIQUE KEY synchronous DELETE end-to-end: INSERT distinct keys -> DELETE a
-- subset by predicate -> count()/SELECT reflect the survivors -> re-running the
-- same DELETE is idempotent. The row finder's internal SELECT rides the
-- read-path delete-bitmap filter, so already-dead rows are excluded from the
-- second pass and no new rows are marked.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;
-- count() must consult the delete bitmap, not the raw rows_count shortcut or an
-- implicit projection (both ignore the per-part delete bitmap).
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS uk_del;

CREATE TABLE uk_del (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

-- Without merge-side delete-bitmap forwarding, a merge would coalesce parts and
-- drop their delete bitmaps; pin the parts so the synchronous DELETE result
-- stays observable.
SYSTEM STOP MERGES uk_del;

-- 5 rows, all-distinct keys (intra-block / cross-part dedup is a later PR; with
-- distinct keys every inserted row is live).
INSERT INTO uk_del VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');

SELECT 'after_insert' AS step, count() FROM uk_del;  -- 5

-- DELETE a subset (ids 2 and 4) by predicate.
DELETE FROM uk_del WHERE id % 2 = 0;

SELECT 'after_delete' AS step, count() FROM uk_del;  -- 3
SELECT 'survivors' AS step, id, v FROM uk_del ORDER BY id;  -- 1,3,5

-- Idempotency: the same predicate again must mark zero new rows (already-dead
-- rows are filtered from the row finder's internal SELECT). count() unchanged.
DELETE FROM uk_del WHERE id % 2 = 0;

SELECT 'after_redelete' AS step, count() FROM uk_del;  -- 3 (unchanged)
SELECT 'survivors_redelete' AS step, id, v FROM uk_del ORDER BY id;  -- 1,3,5

-- A full-scan aggregate (not the trivial-count shortcut) agrees with count().
SELECT 'survivor_sum' AS step, sum(id) FROM uk_del;  -- 1 + 3 + 5 = 9

DROP TABLE uk_del;
