-- Tags: no-random-merge-tree-settings, no-random-settings

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/99578
--
-- A TOCTOU race between SYSTEM STOP MERGES / SYSTEM START MERGES could let a
-- vertical merge continue after the horizontal stage processed zero rows,
-- triggering:
--   "Number of rows in source parts ... differs from number of bytes written
--    to rows_sources file ... It is a bug."
--
-- The fix latches the cancellation at both decision points (`executeImpl` and
-- `executeVerticalMergeForOneColumn`) so that `checkOperationIsNotCanceled`
-- reliably throws ABORTED regardless of the blocker's current state.

DROP TABLE IF EXISTS test_vertical_merge_race;
CREATE TABLE test_vertical_merge_race (
    key UInt64,
    v1 String, v2 String, v3 String, v4 String,
    v5 String, v6 String, v7 String, v8 String,
    v9 String, v10 String, v11 String, v12 String
)
ENGINE = MergeTree ORDER BY key
SETTINGS
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES test_vertical_merge_race;

-- Create multiple small parts
INSERT INTO test_vertical_merge_race SELECT number,       'a','b','c','d','e','f','g','h','i','j','k','l' FROM numbers(100);
INSERT INTO test_vertical_merge_race SELECT number + 100, 'a','b','c','d','e','f','g','h','i','j','k','l' FROM numbers(100);
INSERT INTO test_vertical_merge_race SELECT number + 200, 'a','b','c','d','e','f','g','h','i','j','k','l' FROM numbers(100);
INSERT INTO test_vertical_merge_race SELECT number + 300, 'a','b','c','d','e','f','g','h','i','j','k','l' FROM numbers(100);
INSERT INTO test_vertical_merge_race SELECT number + 400, 'a','b','c','d','e','f','g','h','i','j','k','l' FROM numbers(100);
INSERT INTO test_vertical_merge_race SELECT number + 500, 'a','b','c','d','e','f','g','h','i','j','k','l' FROM numbers(100);
INSERT INTO test_vertical_merge_race SELECT number + 600, 'a','b','c','d','e','f','g','h','i','j','k','l' FROM numbers(100);
INSERT INTO test_vertical_merge_race SELECT number + 700, 'a','b','c','d','e','f','g','h','i','j','k','l' FROM numbers(100);
INSERT INTO test_vertical_merge_race SELECT number + 800, 'a','b','c','d','e','f','g','h','i','j','k','l' FROM numbers(100);
INSERT INTO test_vertical_merge_race SELECT number + 900, 'a','b','c','d','e','f','g','h','i','j','k','l' FROM numbers(100);

-- Rapidly toggle STOP / START to maximise the chance of hitting the TOCTOU window
-- inside MergeTask::executeImpl and executeVerticalMergeForOneColumn.
-- Background merges are triggered each time merges
-- are re-enabled.
SYSTEM START MERGES test_vertical_merge_race;
SYSTEM STOP MERGES test_vertical_merge_race;
SYSTEM START MERGES test_vertical_merge_race;
SYSTEM STOP MERGES test_vertical_merge_race;
SYSTEM START MERGES test_vertical_merge_race;
SYSTEM STOP MERGES test_vertical_merge_race;
SYSTEM START MERGES test_vertical_merge_race;
SYSTEM STOP MERGES test_vertical_merge_race;
SYSTEM START MERGES test_vertical_merge_race;
SYSTEM STOP MERGES test_vertical_merge_race;
SYSTEM START MERGES test_vertical_merge_race;

-- Final merge should succeed without assertion failure
OPTIMIZE TABLE test_vertical_merge_race FINAL;

SELECT count() FROM test_vertical_merge_race;

DROP TABLE test_vertical_merge_race;
