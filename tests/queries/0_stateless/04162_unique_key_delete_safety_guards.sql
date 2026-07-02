-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage, no-fasttest
-- UNIQUE KEY DELETE safety guards (clickhouse-gh review blockers):
--   B4: count() must subtract the delete bitmap even with the IMPLICIT
--       _minmax_count_projection enabled (the default).
--   B5: column-rewrite mutations (MATERIALIZE / CLEAR COLUMN) drop the bitmap
--       sidecars -> rejected for UK tables, key or non-key column.
--   B7: UK DELETE inside an MVCC transaction can't roll back -> rejected.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS uk_guard;

CREATE TABLE uk_guard (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

SYSTEM STOP MERGES uk_guard;

INSERT INTO uk_guard VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');

DELETE FROM uk_guard WHERE id % 2 = 0;  -- removes 2, 4

-- ============================================================================
-- B4: count() with the IMPLICIT minmax-count projection ON (the default) must
--     still reflect the deletes. Pre-fix the implicit projection slipped past
--     the `hasProjections()` half of the guard and overcounted.
-- ============================================================================
SET optimize_use_implicit_projections = 1;
SET optimize_trivial_count_query = 1;
SELECT 'b4_count_implicit_proj' AS step, count() FROM uk_guard;  -- 3
SELECT 'b4_count_pred_implicit_proj' AS step, count() FROM uk_guard WHERE id >= 1;  -- 3

-- ============================================================================
-- B5: MATERIALIZE / CLEAR COLUMN on a UK table (non-key column `v`) must be
--     rejected — they rewrite the part and drop the delete-bitmap sidecars.
-- ============================================================================
ALTER TABLE uk_guard MATERIALIZE COLUMN v; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_guard CLEAR COLUMN v; -- { serverError SUPPORT_IS_DISABLED }

-- ============================================================================
-- B7: UK DELETE inside a transaction is rejected (no rollback for marker parts).
-- ============================================================================
DELETE FROM uk_guard WHERE id = 1 SETTINGS implicit_transaction = 1; -- { serverError SUPPORT_IS_DISABLED }

-- The rejected operations changed nothing: survivors are still 1, 3, 5.
SELECT 'survivors_unchanged' AS step, id FROM uk_guard ORDER BY id;  -- 1,3,5

DROP TABLE uk_guard;

-- ============================================================================
-- B2: SELECT ... FINAL on a UK table with query_plan_optimize_lazy_final = 1
--     must use the single-snapshot read path (lazy-final disabled for UK) and
--     return the post-DELETE survivors. (Lazy-final would build extra
--     ReadFromMergeTree reads, each pinning its own CSN.)
-- ============================================================================
DROP TABLE IF EXISTS uk_final;

CREATE TABLE uk_final (id UInt64, v String)
ENGINE = ReplacingMergeTree
UNIQUE KEY (id)
ORDER BY (id);

SYSTEM STOP MERGES uk_final;

INSERT INTO uk_final VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f');

DELETE FROM uk_final WHERE id % 2 = 0;  -- removes 2, 4, 6

SET query_plan_optimize_lazy_final = 1;
SELECT 'b2_final_survivors' AS step, id FROM uk_final FINAL WHERE id < 100 ORDER BY id;  -- 1,3,5
SELECT 'b2_final_count' AS step, count() FROM uk_final FINAL WHERE id < 100 SETTINGS optimize_trivial_count_query = 0;  -- 3

DROP TABLE uk_final;
