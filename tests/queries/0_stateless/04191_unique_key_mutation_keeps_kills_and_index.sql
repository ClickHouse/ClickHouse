-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree
-- UNIQUE KEY: a mutation keeps every row, moves its source's delete bitmap onto the result and keeps the dense index.
-- Each mutation path: red if the commit does not move the bitmap (dead rows return), or the result loses the
-- dense index (the reprobe, before any re-attach, which would rebuild it, finds no key).
--   1. RENAME COLUMN over a wide part hardlinks the untouched files
--   2. MODIFY COLUMN over a compact part rewrites it
--   3. MODIFY COLUMN to Nullable clones the untouched part
--   4. no source's bitmap file reaches its result, listed after a re-attach: red if the hardlink (1) or the clone (3) takes them
--   5. DELETE after the mutation applies, and OPTIMIZE FINAL absorbs the moved bitmap

SET enable_unique_key = 1;
SET mutations_sync = 2;
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

-- 1. RENAME COLUMN, wide. `rows` counts the 25 dead rows the result keeps.
DROP TABLE IF EXISTS uk_mut_wide;
CREATE TABLE uk_mut_wide (id UInt64, v String, w UInt32)
ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id)
SETTINGS min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO uk_mut_wide SELECT number, 'v1', 1 FROM numbers(100);
INSERT INTO uk_mut_wide SELECT number, 'v2', 2 FROM numbers(80, 20);
DELETE FROM uk_mut_wide WHERE id < 5;

SELECT 'wide before', count(), countDistinct(id), sum(w) FROM uk_mut_wide;
ALTER TABLE uk_mut_wide RENAME COLUMN v TO v_renamed;
SELECT 'wide after', count(), countDistinct(id), sum(w) FROM uk_mut_wide;
SELECT 'wide rows', sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'uk_mut_wide' AND active;
INSERT INTO uk_mut_wide SELECT number, 'v3', 3 FROM numbers(90, 20);
SELECT 'wide reprobe', count(), countDistinct(id) FROM uk_mut_wide;

-- 2. MODIFY COLUMN, compact.
DROP TABLE IF EXISTS uk_mut_compact;
CREATE TABLE uk_mut_compact (id UInt64, v String, w UInt32)
ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id)
SETTINGS min_bytes_for_wide_part = 1000000000, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO uk_mut_compact SELECT number, 'v1', 1 FROM numbers(100);
INSERT INTO uk_mut_compact SELECT number, 'v2', 2 FROM numbers(80, 20);
DELETE FROM uk_mut_compact WHERE id < 5;

SELECT 'compact before', count(), countDistinct(id), sum(w) FROM uk_mut_compact;
ALTER TABLE uk_mut_compact MODIFY COLUMN w UInt64;
SELECT 'compact after', count(), countDistinct(id), sum(w) FROM uk_mut_compact;
SELECT 'compact rows', sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'uk_mut_compact' AND active;
INSERT INTO uk_mut_compact SELECT number, 'v3', 3 FROM numbers(90, 20);
SELECT 'compact reprobe', count(), countDistinct(id) FROM uk_mut_compact;

-- 3. MODIFY COLUMN to Nullable, clone. No implicit statistics, which would force the rewrite.
DROP TABLE IF EXISTS uk_mut_clone;
CREATE TABLE uk_mut_clone (id UInt64, v String, w UInt32)
ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id)
SETTINGS min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1,
         auto_statistics_types = '';

INSERT INTO uk_mut_clone SELECT number, 'v1', 1 FROM numbers(100);
INSERT INTO uk_mut_clone SELECT number, 'v2', 2 FROM numbers(80, 20);
DELETE FROM uk_mut_clone WHERE id < 5;

ALTER TABLE uk_mut_clone MODIFY COLUMN w Nullable(UInt32);
SELECT 'clone after', count(), countDistinct(id), sum(w) FROM uk_mut_clone;
SELECT 'clone rows', sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'uk_mut_clone' AND active;
SYSTEM FLUSH LOGS part_log;
SELECT 'clone untouched parts', sum(ProfileEvents['MutationUntouchedParts']) FROM system.part_log
WHERE database = currentDatabase() AND table = 'uk_mut_clone' AND event_type = 'MutatePart';
INSERT INTO uk_mut_clone SELECT number, 'v3', 3 FROM numbers(90, 20);
SELECT 'clone reprobe', count(), countDistinct(id) FROM uk_mut_clone;

-- 4. The mutation results (`_4`), staged names only: a carried one carries a csn. The source all_2_2_0 holds `for_all_1_1_0`.
DETACH TABLE uk_mut_wide;
ATTACH TABLE uk_mut_wide;
SELECT 'wide bitmaps', name, arrayFilter(x -> startsWith(x, 'for_'), unique_key_bitmap_versions)
FROM system.parts WHERE database = currentDatabase() AND table = 'uk_mut_wide' AND active AND name LIKE '%_4' ORDER BY name;

DETACH TABLE uk_mut_clone;
ATTACH TABLE uk_mut_clone;
SELECT 'clone bitmaps', name, arrayFilter(x -> startsWith(x, 'for_'), unique_key_bitmap_versions)
FROM system.parts WHERE database = currentDatabase() AND table = 'uk_mut_clone' AND active AND name LIKE '%_4' ORDER BY name;

-- 5. DELETE and OPTIMIZE FINAL after the mutation.
DELETE FROM uk_mut_wide WHERE id IN (50, 95);
SELECT 'wide delete', count(), countDistinct(id), sum(w) FROM uk_mut_wide;
OPTIMIZE TABLE uk_mut_wide FINAL;
SELECT 'wide optimized', count(), sum(w) FROM uk_mut_wide;
SELECT 'wide optimized rows', sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'uk_mut_wide' AND active;

DROP TABLE uk_mut_wide;
DROP TABLE uk_mut_compact;
DROP TABLE uk_mut_clone;
