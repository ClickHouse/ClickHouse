-- Tests the behavior of MergeTree setting 'secondary_indices_on_columns_alter_modify' with tables in compact and wide format

DROP TABLE IF EXISTS test_compact;
DROP TABLE IF EXISTS test_wide;

CREATE TABLE test_compact (
    a Int32,
    b Int32,
    c Int32,
    INDEX idx_minmax b TYPE minmax,
    INDEX idx_set c TYPE set(100),
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 999999999;

CREATE TABLE test_wide (
    a Int32,
    b Int32,
    c Int32,
    INDEX idx_minmax b TYPE minmax,
    INDEX idx_set c TYPE set(100),
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO test_compact VALUES (1, 1, 1);
INSERT INTO test_wide VALUES (1, 1, 1);

SELECT 'Check default behavior';

ALTER TABLE test_compact MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test_wide MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT 'Check behavior with THROW';

ALTER TABLE test_compact MODIFY SETTING secondary_indices_on_columns_alter_modify = 'throw';
ALTER TABLE test_wide MODIFY SETTING secondary_indices_on_columns_alter_modify = 'throw';

-- ALTER TABLE MODIFY COLUMN is expected to throw
ALTER TABLE test_compact MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test_wide MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- It must be possible to change the column default value even with THROW
ALTER TABLE test_compact MODIFY COLUMN b DEFAULT '123';
ALTER TABLE test_wide MODIFY COLUMN b DEFAULT '123';

-- ALTER TABLE UPDATE must be possible even with THROW
ALTER TABLE test_compact UPDATE b = 2 WHERE b = 1 SETTINGS mutations_sync = 2;
SELECT b from test_compact;
ALTER TABLE test_wide UPDATE b = 2 WHERE b = 1 SETTINGS mutations_sync = 2;
SELECT b from test_wide;

SELECT 'Check behavior with DROP';

ALTER TABLE test_compact MODIFY SETTING secondary_indices_on_columns_alter_modify = 'drop';
ALTER TABLE test_wide MODIFY SETTING secondary_indices_on_columns_alter_modify = 'drop';

-- ALTER TABLE MODIFY COLUMN must work now and the indexes must be dropped
ALTER TABLE test_compact MODIFY COLUMN b String;
ALTER TABLE test_wide MODIFY COLUMN b String;

SELECT marks_bytes == 0 FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT marks_bytes == 0 FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

SELECT 'Check behavior with REBUILD';

ALTER TABLE test_compact MODIFY SETTING secondary_indices_on_columns_alter_modify = 'rebuild';
ALTER TABLE test_wide MODIFY SETTING secondary_indices_on_columns_alter_modify = 'rebuild';

-- Expect that ALTER TABLE MODIFY COLUMN works and the indexes must be rebuild
ALTER TABLE test_compact MODIFY COLUMN b Int32;
ALTER TABLE test_wide MODIFY COLUMN b Int32;

SELECT marks_bytes > 0 FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT marks_bytes > 0 FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

SELECT 'Check behavior with IGNORE';

ALTER TABLE test_compact MODIFY SETTING secondary_indices_on_columns_alter_modify = 'ignore';
ALTER TABLE test_wide MODIFY SETTING secondary_indices_on_columns_alter_modify = 'ignore';

ALTER TABLE test_compact MODIFY COLUMN b String;
ALTER TABLE test_wide MODIFY COLUMN b String;

DROP TABLE test_compact;
DROP TABLE test_wide;
