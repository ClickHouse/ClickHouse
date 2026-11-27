DROP TABLE IF EXISTS mt_04401;
CREATE TABLE mt_04401 (d Date) ENGINE = MergeTree ORDER BY tuple();

-- Updating a column that does not exist must report a clear NO_SUCH_COLUMN_IN_TABLE,
-- not the confusing "Column is updated but not requested to read" it regressed to.
ALTER TABLE mt_04401 UPDATE n = 2 WHERE 1; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SET mutations_sync = 1;
ALTER TABLE mt_04401 UPDATE n = 2 WHERE 1; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

DROP TABLE mt_04401;
