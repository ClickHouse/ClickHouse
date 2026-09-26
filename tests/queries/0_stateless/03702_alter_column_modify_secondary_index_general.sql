-- Tests the behavior of MergeTree setting 'alter_column_secondary_index_mode' with tables in compact and wide format
-- for UPDATE MODIFY COLUMN operations.

SET apply_mutations_on_fly = 0;
SET mutations_sync = 1;
SET alter_sync = 1;

DROP TABLE IF EXISTS test_compact;
DROP TABLE IF EXISTS test_wide;

CREATE TABLE test_compact (
    a Int32,
    b Int32,
    c Int32,
    INDEX idx_minmax b TYPE minmax
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 999999999;

CREATE TABLE test_wide (
    a Int32,
    b Int32,
    c Int32,
    INDEX idx_minmax b TYPE minmax
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO test_compact VALUES (1, 1, 4);
INSERT INTO test_wide VALUES (1, 1, 4);

SELECT 'Check behavior with THROW';

ALTER TABLE test_compact MODIFY SETTING alter_column_secondary_index_mode = 'throw';
ALTER TABLE test_wide MODIFY SETTING alter_column_secondary_index_mode = 'throw';

-- ALTER TABLE MODIFY COLUMN is expected to throw
ALTER TABLE test_compact MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test_wide MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- However, it must be possible to change the column default value without an exception
ALTER TABLE test_compact MODIFY COLUMN b DEFAULT 123;
ALTER TABLE test_wide MODIFY COLUMN b DEFAULT 123;

-- It's also possible to alter other columns that don't have secondary indexes
ALTER TABLE test_compact MODIFY COLUMN c String;

SELECT 'Check behavior with COMPATIBILITY';

ALTER TABLE test_compact MODIFY SETTING alter_column_secondary_index_mode = 'compatibility';
ALTER TABLE test_wide MODIFY SETTING alter_column_secondary_index_mode = 'compatibility';

-- ALTER TABLE MODIFY COLUMN is expected to throw
ALTER TABLE test_compact MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test_wide MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- However, it must be possible to change the column default value without an exception
ALTER TABLE test_compact MODIFY COLUMN b DEFAULT 123;
ALTER TABLE test_wide MODIFY COLUMN b DEFAULT 123;

SELECT 'Check behavior with REBUILD'; -- that's the default

ALTER TABLE test_compact MODIFY SETTING alter_column_secondary_index_mode = 'rebuild';
ALTER TABLE test_wide MODIFY SETTING alter_column_secondary_index_mode = 'rebuild';

-- Expect that ALTER TABLE MODIFY COLUMN works and the indexes must be rebuild
ALTER TABLE test_compact MODIFY COLUMN b Int32;
ALTER TABLE test_wide MODIFY COLUMN b Int32;

SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

SELECT 'Check behavior with DROP';

ALTER TABLE test_compact MODIFY SETTING alter_column_secondary_index_mode = 'drop';
ALTER TABLE test_wide MODIFY SETTING alter_column_secondary_index_mode = 'drop';

-- ALTER TABLE MODIFY COLUMN must work now and the indexes must be dropped
ALTER TABLE test_compact MODIFY COLUMN b String;
ALTER TABLE test_wide MODIFY COLUMN b String;

SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

-- Check that changing the column default value still works
ALTER TABLE test_compact MODIFY COLUMN b DEFAULT '321';
ALTER TABLE test_wide MODIFY COLUMN b DEFAULT '321';

SELECT 'Check REBUILD after DROP (parts without index)';

ALTER TABLE test_compact MODIFY SETTING alter_column_secondary_index_mode = 'rebuild';
ALTER TABLE test_wide MODIFY SETTING alter_column_secondary_index_mode = 'rebuild';

-- Expect that ALTER TABLE MODIFY COLUMN works and the indexes must be rebuild
ALTER TABLE test_compact MODIFY COLUMN b Int32;
ALTER TABLE test_wide MODIFY COLUMN b Int32;

SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

DROP TABLE test_compact;
DROP TABLE test_wide;