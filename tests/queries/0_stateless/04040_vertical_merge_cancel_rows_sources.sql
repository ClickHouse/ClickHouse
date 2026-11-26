-- Regression test for the assertion:
-- "Number of rows in source parts (N) excluding filtered rows (0) differs from
--  number of bytes written to rows_sources file (0). It is a bug."
-- The bug: when SYSTEM STOP/START MERGES toggles rapidly, the horizontal merge
-- stage can be cancelled (is_cancelled() returns true) but the cancellation check
-- in finalize() passes because the blocker was released in between. The merge then
-- proceeds to the vertical stage with rows_sources_count=0, hitting the assertion.
-- The fix: once cancellation is detected, persist it in merge_list_element->is_cancelled
-- so that subsequent checks (finalize, vertical stage) also see the cancellation
-- even if the merge blocker is released in between.

DROP TABLE IF EXISTS test;
CREATE TABLE test (id UInt64, d Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
         vertical_merge_algorithm_min_rows_to_activate = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES test;

-- Create 20 small parts
INSERT INTO test SELECT number, number FROM numbers(10);
INSERT INTO test SELECT number + 10, toString(number) FROM numbers(10);
INSERT INTO test SELECT number + 20, number FROM numbers(10);
INSERT INTO test SELECT number + 30, toString(number) FROM numbers(10);
INSERT INTO test SELECT number + 40, number FROM numbers(10);
INSERT INTO test SELECT number + 50, toString(number) FROM numbers(10);
INSERT INTO test SELECT number + 60, number FROM numbers(10);
INSERT INTO test SELECT number + 70, toString(number) FROM numbers(10);
INSERT INTO test SELECT number + 80, number FROM numbers(10);
INSERT INTO test SELECT number + 90, toString(number) FROM numbers(10);
INSERT INTO test SELECT number + 100, number FROM numbers(10);
INSERT INTO test SELECT number + 110, toString(number) FROM numbers(10);
INSERT INTO test SELECT number + 120, number FROM numbers(10);
INSERT INTO test SELECT number + 130, toString(number) FROM numbers(10);
INSERT INTO test SELECT number + 140, number FROM numbers(10);
INSERT INTO test SELECT number + 150, toString(number) FROM numbers(10);
INSERT INTO test SELECT number + 160, number FROM numbers(10);
INSERT INTO test SELECT number + 170, toString(number) FROM numbers(10);
INSERT INTO test SELECT number + 180, number FROM numbers(10);
INSERT INTO test SELECT number + 190, toString(number) FROM numbers(10);

-- Rapidly toggle merges to trigger the race condition
SYSTEM START MERGES test;
SYSTEM STOP MERGES test;
SYSTEM START MERGES test;
SYSTEM STOP MERGES test;
SYSTEM START MERGES test;
SYSTEM STOP MERGES test;
SYSTEM START MERGES test;
SYSTEM STOP MERGES test;
SYSTEM START MERGES test;
SYSTEM STOP MERGES test;
SYSTEM START MERGES test;

-- Final merge should succeed without assertion failure
OPTIMIZE TABLE test FINAL;

SELECT count() FROM test;

DROP TABLE test;
