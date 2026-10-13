-- Vertical merge should not fail when a dropped column is still physically present in parts.
-- `injectRequiredColumns` must not pick a dropped column as the minimum-size column to inject.
--
-- The bug triggers during vertical merge when reading a "gathering" column that has no
-- physical file in the part (added after insertion with constant DEFAULT). Since the default
-- has no column dependencies, `have_at_least_one_physical_column` remains false, and
-- `injectRequiredColumns` picks the minimum-size physical column. Without the fix, it may
-- pick a dropped column still on disk, causing `NO_SUCH_COLUMN_IN_TABLE`.

DROP TABLE IF EXISTS data;

CREATE TABLE data (key UInt64, h UInt8 DEFAULT 0)
ENGINE = MergeTree()
ORDER BY key
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 0;

-- Insert enough rows so that `h` (UInt8) is clearly the smallest column by compressed size.
-- With just 1-2 rows, the compression overhead makes all columns equal in size.
INSERT INTO data SELECT number, 0 FROM numbers(10000);
INSERT INTO data SELECT number + 10000, 0 FROM numbers(10000);

-- Add a column `value` that will NOT be physically present in existing parts.
-- Its default is a constant — no column dependencies.
ALTER TABLE data ADD COLUMN value UInt64 DEFAULT 42;

-- Prevent the background thread from applying the mutation.
SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

-- Drop `h` with alter_sync = 0 so the ALTER returns immediately.
-- The mutation is queued but not applied — parts still have `h` on disk.
SET alter_sync = 0;
ALTER TABLE data DROP COLUMN h;

-- Trigger vertical merge. The merge reads original parts (which still have `h`),
-- applying the DROP on-the-fly via alter_conversions. Reading the gathering column
-- `value` (not physical, constant default) makes `have_at_least_one_physical_column`
-- false, so `injectRequiredColumns` picks the minimum-size column from part's columns.
-- Without the fix, it picks dropped `h` → `NO_SUCH_COLUMN_IN_TABLE`.
OPTIMIZE TABLE data FINAL;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SELECT count(), min(key), max(key), min(value), max(value) FROM data;

DROP TABLE data;
