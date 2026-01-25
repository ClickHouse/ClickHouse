-- Test for the fix of "Could not find a column of minimum size in MergeTree" exception.
-- This happens when part's columns don't match table metadata columns after schema changes,
-- and we need to find a minimum size column to count rows.

DROP TABLE IF EXISTS t_min_size_column;

-- Use tuple() as ORDER BY to allow flexible schema changes
CREATE TABLE t_min_size_column (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();

-- Insert data to create a part with columns `a` and `b`
INSERT INTO t_min_size_column VALUES (1, 10), (2, 20), (3, 30);

-- Add a new column with default value (old parts won't have files for this column)
ALTER TABLE t_min_size_column ADD COLUMN c UInt64 DEFAULT 0;

-- Drop original column `a` (keep `b` so the part is not empty)
-- Now table metadata has columns `b` and `c`, but the old part has columns `a` and `b`
ALTER TABLE t_min_size_column DROP COLUMN a;

-- Select only virtual columns - this requires finding a minimum size column to count rows
-- Without the fix, if metadata column `c` doesn't exist in the part, it would throw:
-- "Could not find a column of minimum size in MergeTree"
SELECT count() FROM t_min_size_column;

-- Also test OPTIMIZE which triggers merge code path
OPTIMIZE TABLE t_min_size_column FINAL;

SELECT count() FROM t_min_size_column;

DROP TABLE t_min_size_column;
