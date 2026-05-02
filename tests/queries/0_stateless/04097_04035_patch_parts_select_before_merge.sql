-- Test for issue where SELECT fails when patch parts are not yet merged
-- This test verifies that SELECT should work correctly even when
-- there are unmerged patch parts, including with lazy materialization enabled

SET enable_lightweight_update = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_patch_select;

CREATE TABLE t_patch_select(a UInt64, b UInt64 DEFAULT 0, c String DEFAULT '')
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

-- Insert initial data
INSERT INTO t_patch_select (a) SELECT number FROM numbers(100);

-- Create patch parts via UPDATE (without merging)
UPDATE t_patch_select SET b = 1 WHERE a % 2 = 0;

-- This SELECT should work even though patches are not merged
SELECT count() FROM t_patch_select;
SELECT * FROM t_patch_select WHERE a < 10 ORDER BY a;

-- Verify the data is correct
SELECT count() FROM t_patch_select WHERE b = 1;

-- Test with lazy materialization enabled (this was causing issues before the fix)
SET query_plan_optimize_lazy_materialization = 1;
SELECT count() FROM t_patch_select;
SELECT * FROM t_patch_select WHERE a < 10 ORDER BY a;

DROP TABLE t_patch_select;