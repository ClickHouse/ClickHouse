-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree
-- UNIQUE KEY: a merge keeps every live row and drops only the rows dead at its snapshot.
--   1. dead rows: the merge drops exactly the rows a DELETE killed; a second merge drops those of a DELETE on its result
--   2. background: a scheduler-picked merge goes through
--   3. insert cap: a merge ignores `unique_key_max_encoded_size`
--   4. DEDUPLICATE: OPTIMIZE ... DEDUPLICATE and its DRY RUN form are rejected

SET enable_unique_key = 1;
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS uk_merge_delete;

-- 1. dead rows: red if a merge reads its inputs without their bitmaps (the OPTIMIZE aborts
-- the debug server on the merge's row-mapping check).
CREATE TABLE uk_merge_delete (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO uk_merge_delete SELECT number, 'x' FROM numbers(0, 100);
INSERT INTO uk_merge_delete SELECT number, 'y' FROM numbers(100, 100);

DELETE FROM uk_merge_delete WHERE id < 30;

SELECT 'delete_count_before', count() FROM uk_merge_delete;    -- 170

OPTIMIZE TABLE uk_merge_delete FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT 'delete_parts_after', count() FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_merge_delete' AND active; -- 1
SELECT 'delete_count_after', count() FROM uk_merge_delete;     -- 170
SELECT 'delete_distinct', countDistinct(id) FROM uk_merge_delete; -- 170
SELECT 'delete_min', min(id) FROM uk_merge_delete;             -- 30

DELETE FROM uk_merge_delete WHERE id >= 190;
SELECT 'remerge_count_after_delete', count() FROM uk_merge_delete;  -- 160

INSERT INTO uk_merge_delete SELECT number, 'r' FROM numbers(200, 10);
OPTIMIZE TABLE uk_merge_delete FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'remerge_count', count() FROM uk_merge_delete;          -- 170
SELECT 'remerge_max', max(id) FROM uk_merge_delete;            -- 209
SELECT 'remerge_gone', count() FROM uk_merge_delete WHERE id >= 190 AND id < 200; -- 0

-- 2. background: red if a scheduled merge treats the scheduler's transaction as
-- the user's (the merge is refused and `SYSTEM SYNC MERGES` times out).
DROP TABLE IF EXISTS uk_merge_background;

CREATE TABLE uk_merge_background (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS min_bytes_for_wide_part = 0, merge_selector_algorithm = 'Manual';

INSERT INTO uk_merge_background SELECT number, 'p' FROM numbers(0, 40);
INSERT INTO uk_merge_background SELECT number, 'q' FROM numbers(40, 40);

SELECT 'bg_parts_before', count() FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_merge_background' AND active; -- 2

SET max_execution_time = 60;
SYSTEM SCHEDULE MERGE uk_merge_background PARTS 'all_1_1_0', 'all_2_2_0';
SYSTEM SYNC MERGES uk_merge_background;
SET max_execution_time = 0;

SELECT 'bg_parts_after', count() FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_merge_background' AND active; -- 1
SELECT 'bg_count', count() FROM uk_merge_background;             -- 80
SELECT 'bg_distinct', countDistinct(id) FROM uk_merge_background; -- 80

-- 3. insert cap: red if the merge's dense-index write applies the INSERT cap (BAD_ARGUMENTS).
-- The merge sees the global default of 256, hence 400-byte keys; vertical-merge thresholds are pinned.
DROP TABLE IF EXISTS uk_encoded_size_merge;

CREATE TABLE uk_encoded_size_merge (k String, v UInt64)
ENGINE = MergeTree ORDER BY k UNIQUE KEY k
SETTINGS min_bytes_for_wide_part = 0,
         vertical_merge_algorithm_min_rows_to_activate = 131072, vertical_merge_algorithm_min_columns_to_activate = 11;

SYSTEM STOP MERGES uk_encoded_size_merge;

INSERT INTO uk_encoded_size_merge SELECT repeat('k', 400) || toString(number), number FROM numbers(50) SETTINGS unique_key_max_encoded_size = 4096;
INSERT INTO uk_encoded_size_merge SELECT repeat('k', 400) || toString(number + 50), number FROM numbers(50) SETTINGS unique_key_max_encoded_size = 4096;
SELECT 'cap_before_merge', count(), count(DISTINCT k) FROM uk_encoded_size_merge;

-- The cap still refuses a new row below it, so the cap itself is live.
INSERT INTO uk_encoded_size_merge SELECT repeat('k', 400) || toString(number + 500), number FROM numbers(1) SETTINGS unique_key_max_encoded_size = 8; -- { serverError BAD_ARGUMENTS }

SYSTEM START MERGES uk_encoded_size_merge;
OPTIMIZE TABLE uk_encoded_size_merge FINAL;

SELECT 'cap_after_merge', count(), count(DISTINCT k) FROM uk_encoded_size_merge;

DROP TABLE uk_encoded_size_merge;

-- 4. DEDUPLICATE: red if OPTIMIZE ... DEDUPLICATE or its DRY RUN form stops being refused.
DROP TABLE IF EXISTS uk_merge_dedup;
CREATE TABLE uk_merge_dedup (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES uk_merge_dedup;
INSERT INTO uk_merge_dedup VALUES (1000, 'x');
INSERT INTO uk_merge_dedup VALUES (1001, 'y');
OPTIMIZE TABLE uk_merge_dedup FINAL DEDUPLICATE; -- { serverError SUPPORT_IS_DISABLED }
OPTIMIZE TABLE uk_merge_dedup DRY RUN PARTS 'all_1_1_0', 'all_2_2_0' DEDUPLICATE; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE uk_merge_dedup;

DROP TABLE uk_merge_delete;
DROP TABLE uk_merge_background;
