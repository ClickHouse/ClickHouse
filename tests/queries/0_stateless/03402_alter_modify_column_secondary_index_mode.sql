-- Tests the behavior of MergeTree setting 'alter_modify_column_secondary_index_mode' with tables in compact and wide format

DROP TABLE IF EXISTS test_compact;
DROP TABLE IF EXISTS test_wide;

CREATE TABLE test_compact (
    a Int32,
    b Int32,
    INDEX idx_minmax b TYPE minmax
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 999999999;

CREATE TABLE test_wide (
    a Int32,
    b Int32,
    INDEX idx_minmax b TYPE minmax
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO test_compact VALUES (1, 1);
INSERT INTO test_wide VALUES (1, 1);

SELECT 'Check behavior with THROW';

ALTER TABLE test_compact MODIFY SETTING alter_modify_column_secondary_index_mode = 'throw';
ALTER TABLE test_wide MODIFY SETTING alter_modify_column_secondary_index_mode = 'throw';

-- ALTER TABLE MODIFY COLUMN is expected to throw
ALTER TABLE test_compact MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test_wide MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- However, it must be possible to change the column default value without an exception
ALTER TABLE test_compact MODIFY COLUMN b DEFAULT 123;
ALTER TABLE test_wide MODIFY COLUMN b DEFAULT 123;

-- Also, ALTER TABLE UPDATE must be possible without an exception
ALTER TABLE test_compact UPDATE b = 2 WHERE b = 1 SETTINGS mutations_sync = 2;
SELECT * from test_compact;
ALTER TABLE test_wide UPDATE b = 2 WHERE b = 1 SETTINGS mutations_sync = 2;
SELECT * from test_wide;

SELECT 'Check behavior with DROP';

ALTER TABLE test_compact MODIFY SETTING alter_modify_column_secondary_index_mode = 'drop';
ALTER TABLE test_wide MODIFY SETTING alter_modify_column_secondary_index_mode = 'drop';

-- ALTER TABLE MODIFY COLUMN must work now and the indexes must be dropped
ALTER TABLE test_compact MODIFY COLUMN b String;
ALTER TABLE test_wide MODIFY COLUMN b String;

SELECT marks_bytes == 0 FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT marks_bytes == 0 FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

-- Check that changing the column default value still works
ALTER TABLE test_compact MODIFY COLUMN b DEFAULT '321';
ALTER TABLE test_wide MODIFY COLUMN b DEFAULT '321';

-- Check that ALTER TABLE UPDATE still works
ALTER TABLE test_compact UPDATE b = '3' WHERE b = '2' SETTINGS mutations_sync = 2;
SELECT * from test_compact;
ALTER TABLE test_wide UPDATE b = '3' WHERE b = '2' SETTINGS mutations_sync = 2;
SELECT * from test_wide;

SELECT 'Check behavior with REBUILD'; -- that's the default

ALTER TABLE test_compact MODIFY SETTING alter_modify_column_secondary_index_mode = 'rebuild';
ALTER TABLE test_wide MODIFY SETTING alter_modify_column_secondary_index_mode = 'rebuild';

-- Expect that ALTER TABLE MODIFY COLUMN works and the indexes must be rebuild
ALTER TABLE test_compact MODIFY COLUMN b Int32;
ALTER TABLE test_wide MODIFY COLUMN b Int32;

SELECT marks_bytes > 0 FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT marks_bytes > 0 FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

-- Check that changing the column default value still works
ALTER TABLE test_compact MODIFY COLUMN b DEFAULT 321;
ALTER TABLE test_wide MODIFY COLUMN b DEFAULT 321;

-- Check that ALTER TABLE UPDATE still works
ALTER TABLE test_compact UPDATE b = 4 WHERE b = 3 SETTINGS mutations_sync = 2;
SELECT * from test_compact;
ALTER TABLE test_wide UPDATE b = 4 WHERE b = 3 SETTINGS mutations_sync = 2;
SELECT * from test_wide;

SELECT 'Check behavior with IGNORE'; -- breaks correctness of queries, only for debugging purposes

ALTER TABLE test_compact MODIFY SETTING alter_modify_column_secondary_index_mode = 'ignore';
ALTER TABLE test_wide MODIFY SETTING alter_modify_column_secondary_index_mode = 'ignore';

ALTER TABLE test_compact MODIFY COLUMN b String;
ALTER TABLE test_wide MODIFY COLUMN b String;

DROP TABLE test_compact;
DROP TABLE test_wide;
