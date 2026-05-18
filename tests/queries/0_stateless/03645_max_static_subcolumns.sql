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