-- test the behavior of mergetree setting secondary_indices_on_columns_alter_modify with compact and wide format

DROP TABLE IF EXISTS test_compact;

CREATE TABLE test_compact
(
    a int,
    b int,
    c int,
    INDEX idx_minmax b TYPE minmax,
    INDEX idx_set c TYPE set(100),
)
ENGINE = MergeTree
ORDER BY a
SETTINGS min_bytes_for_wide_part = 999999999;

INSERT INTO test_compact VALUES(1, 1, 1);

-- default
SELECT 'Check default setting with compact:';
ALTER TABLE test_compact MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- throw
ALTER TABLE test_compact MODIFY SETTING secondary_indices_on_columns_alter_modify = 'throw';

SELECT 'Check throw setting with compact:';
ALTER TABLE test_compact MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT 'Check throw setting of change default with compact:';
-- should be changable even in the throw mode
ALTER TABLE test_compact MODIFY COLUMN b DEFAULT '123';

-- throw has no effect on alter table update
ALTER TABLE test_compact UPDATE b = 2 WHERE b = 1 SETTINGS mutations_sync = 2;
SELECT b from test_compact;

-- drop
ALTER TABLE test_compact MODIFY SETTING secondary_indices_on_columns_alter_modify = 'drop';

ALTER TABLE test_compact MODIFY COLUMN b String;

SELECT marks_bytes == 0
FROM system.data_skipping_indices
WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';

-- rebuild
ALTER TABLE test_compact MODIFY SETTING secondary_indices_on_columns_alter_modify = 'rebuild';

ALTER TABLE test_compact MODIFY COLUMN b int;

SELECT marks_bytes > 0
FROM system.data_skipping_indices
WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';

-- ignore
ALTER TABLE test_compact MODIFY SETTING secondary_indices_on_columns_alter_modify = 'ignore';

ALTER TABLE test_compact MODIFY COLUMN b String;

DROP TABLE test_compact;


DROP TABLE IF EXISTS test_wide;

CREATE TABLE test_wide
(
    a int,
    b int,
    c int,
    INDEX idx_minmax b TYPE minmax,
    INDEX idx_set c TYPE set(100),
)
ENGINE = MergeTree
ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO test_wide VALUES(1, 1, 1);

-- default
SELECT 'Check default setting with wide:';
ALTER TABLE test_wide MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- throw
ALTER TABLE test_wide MODIFY SETTING secondary_indices_on_columns_alter_modify = 'throw';

SELECT 'Check throw setting with wide:';
ALTER TABLE test_wide MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT 'Check throw setting of change default with wide:';
-- should be changable even in the throw mode
ALTER TABLE test_wide MODIFY COLUMN b DEFAULT '123';

-- throw has no effect on alter table update
ALTER TABLE test_wide UPDATE b = 2 WHERE b = 1 SETTINGS mutations_sync = 2;
SELECT b from test_wide;

-- drop
ALTER TABLE test_wide MODIFY SETTING secondary_indices_on_columns_alter_modify = 'drop';

ALTER TABLE test_wide MODIFY COLUMN b String;

SELECT marks_bytes == 0
FROM system.data_skipping_indices
WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

-- rebuild
ALTER TABLE test_wide MODIFY SETTING secondary_indices_on_columns_alter_modify = 'rebuild';

ALTER TABLE test_wide MODIFY COLUMN b int;

SELECT marks_bytes > 0
FROM system.data_skipping_indices
WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

-- ignore
ALTER TABLE test_wide MODIFY SETTING secondary_indices_on_columns_alter_modify = 'ignore';

ALTER TABLE test_wide MODIFY COLUMN b String;

DROP TABLE test_wide;
