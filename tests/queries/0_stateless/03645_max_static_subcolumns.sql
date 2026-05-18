SET mutations_sync = 1;
SET max_static_subcolumns = 0;

CREATE TABLE WorksBeforeSubcolumnLimit
(
    `ID` UInt32,
    `Subcolumns` Tuple(subcolumn1 UInt32, subcolumn2 UInt32, subcolumn3 DateTime, subcolumn4 Int64, subcolumn5 String, subcolumn6 UInt32)
)
ENGINE = MergeTree
ORDER BY ID;

SET max_static_subcolumns = 2;

CREATE TABLE FailsAfterSubcolumnLimit
(
    `ID` UInt32,
    `Subcolumns` Tuple(subcolumn1 UInt32, subcolumn2 UInt32, subcolumn3 DateTime, subcolumn4 Int64, subcolumn5 String, subcolumn6 UInt32)
)
ENGINE = MergeTree
ORDER BY ID; -- { serverError TOO_MANY_SUBCOLUMNS }

-- Test ALTER TABLE validation.
DROP TABLE WorksBeforeSubcolumnLimit;

SET max_static_subcolumns = 0;

CREATE TABLE test_alter_subcolumns
(
    `ID` UInt32,
    `col1` UInt32
)
ENGINE = MergeTree
ORDER BY ID;

-- ALTER ADD COLUMN that exceeds the limit should fail.
SET max_static_subcolumns = 2;
ALTER TABLE test_alter_subcolumns ADD COLUMN `nested` Tuple(a UInt32, b UInt32, c UInt32); -- { serverError TOO_MANY_SUBCOLUMNS }

-- ALTER ADD COLUMN within the limit should succeed.
SET max_static_subcolumns = 10;
ALTER TABLE test_alter_subcolumns ADD COLUMN `nested` Tuple(a UInt32, b UInt32, c UInt32);

-- ALTER MODIFY COLUMN that increases subcolumns beyond the limit should fail.
SET max_static_subcolumns = 2;
ALTER TABLE test_alter_subcolumns MODIFY COLUMN `nested` Tuple(a UInt32, b UInt32, c UInt32, d UInt32, e UInt32); -- { serverError TOO_MANY_SUBCOLUMNS }

-- ALTER DROP COLUMN that reduces subcolumns should succeed even with a low limit.
SET max_static_subcolumns = 2;
ALTER TABLE test_alter_subcolumns DROP COLUMN `nested`;

-- Check is disabled when setting is 0.
SET max_static_subcolumns = 0;
ALTER TABLE test_alter_subcolumns ADD COLUMN `big_nested` Tuple(a UInt32, b UInt32, c UInt32, d UInt32, e UInt32);

DROP TABLE test_alter_subcolumns;

-- ALTERs that don't increase the static subcolumn count must succeed
-- even when the table is already over the limit (e.g. created earlier
-- with max_static_subcolumns = 0).
SET max_static_subcolumns = 0;

CREATE TABLE test_alter_over_limit
(
    `ID` UInt32,
    `Subcolumns` Tuple(subcolumn1 UInt32, subcolumn2 UInt32, subcolumn3 DateTime, subcolumn4 Int64, subcolumn5 String, subcolumn6 UInt32)
)
ENGINE = MergeTree
ORDER BY ID;

SET max_static_subcolumns = 2;

-- Unrelated ALTERs on an over-limit table must still succeed.
ALTER TABLE test_alter_over_limit MODIFY COMMENT 'comment after limit lowered';
ALTER TABLE test_alter_over_limit MODIFY SETTING min_bytes_for_wide_part = 0;
ALTER TABLE test_alter_over_limit MODIFY COLUMN `ID` UInt32 COMMENT 'id column';

-- DROP COLUMN must also succeed even though the table remains over the limit afterwards.
ALTER TABLE test_alter_over_limit DROP COLUMN IF EXISTS nonexistent_column;

-- But an ALTER that further increases the static subcolumn count is still rejected.
ALTER TABLE test_alter_over_limit ADD COLUMN `more` Tuple(x UInt32, y UInt32); -- { serverError TOO_MANY_SUBCOLUMNS }

DROP TABLE test_alter_over_limit;