-- Tests the behavior of MergeTree setting 'alter_column_secondary_index_mode' with tables in compact and wide format
-- for ALTER TABLE {} DELETE and ALTER TABLE {} UPDATE

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

SELECT '======== Check behavior with THROW ========';

ALTER TABLE test_compact MODIFY SETTING alter_column_secondary_index_mode = 'throw';
ALTER TABLE test_wide MODIFY SETTING alter_column_secondary_index_mode = 'throw';

-- ALTER TABLE UPDATE are expected to throw
ALTER TABLE test_compact UPDATE b = 3 WHERE b = 1; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE test_compact DELETE WHERE b = 1; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE test_wide UPDATE b = 3 WHERE b = 1; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE test_wide DELETE WHERE b = 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT '======== Check behavior with COMPATIBILITY ========'; -- Same as REBUILD

ALTER TABLE test_compact MODIFY SETTING alter_column_secondary_index_mode = 'compatibility';
ALTER TABLE test_wide MODIFY SETTING alter_column_secondary_index_mode = 'compatibility';

INSERT INTO test_compact VALUES (1, 100, 6);
OPTIMIZE TABLE test_compact FINAL;
INSERT INTO test_wide VALUES (1, 100, 6);
OPTIMIZE TABLE test_wide FINAL;
SELECT 'COMPACT BEFORE', * from test_compact;
SELECT 'WIDE BEFORE', * from test_wide;

ALTER TABLE test_compact UPDATE b = 3 WHERE b = 1;
SELECT 'COMPACT AFTER UPDATE', * from test_compact;
ALTER TABLE test_wide UPDATE b = 3 WHERE b = 1;
SELECT 'WIDE AFTER UPDATE', * from test_wide;

SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

ALTER TABLE test_compact DELETE WHERE b = 100;
SELECT 'COMPACT AFTER DELETE', * from test_compact;
ALTER TABLE test_wide DELETE WHERE b = 100;
SELECT 'WIDE AFTER DELETE', * from test_wide;

SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';


SELECT '======== Check behavior with REBUILD ========';

ALTER TABLE test_compact MODIFY SETTING alter_column_secondary_index_mode = 'compatibility';
ALTER TABLE test_wide MODIFY SETTING alter_column_secondary_index_mode = 'compatibility';

INSERT INTO test_compact VALUES (1, 100, 6);
OPTIMIZE TABLE test_compact FINAL;
INSERT INTO test_wide VALUES (1, 100, 6);
OPTIMIZE TABLE test_wide FINAL;
SELECT 'COMPACT BEFORE', * from test_compact;
SELECT 'WIDE BEFORE', * from test_wide;

ALTER TABLE test_compact UPDATE b = 5 WHERE b = 3;
SELECT 'COMPACT AFTER UPDATE', * from test_compact;
ALTER TABLE test_wide UPDATE b = 5 WHERE b = 3;
SELECT 'WIDE AFTER UPDATE', * from test_wide;

SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

ALTER TABLE test_compact DELETE WHERE b = 100;
SELECT 'COMPACT AFTER DELETE', * from test_compact;
ALTER TABLE test_wide DELETE WHERE b = 100;
SELECT 'WIDE AFTER DELETE', * from test_wide;

SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';


SELECT '======== Check behavior with DROP ========';

ALTER TABLE test_compact MODIFY SETTING alter_column_secondary_index_mode = 'drop';
ALTER TABLE test_wide MODIFY SETTING alter_column_secondary_index_mode = 'drop';

INSERT INTO test_compact VALUES (1, 100, 6);
OPTIMIZE TABLE test_compact FINAL;
INSERT INTO test_wide VALUES (1, 100, 6);
OPTIMIZE TABLE test_wide FINAL;

SELECT 'COMPACT BEFORE', * from test_compact;
SELECT 'WIDE BEFORE', * from test_wide;

ALTER TABLE test_compact UPDATE b = 7 WHERE b = 5;
SELECT 'COMPACT AFTER UPDATE', * from test_compact;
ALTER TABLE test_wide UPDATE b = 7 WHERE b = 5;
SELECT 'WIDE AFTER UPDATE', * from test_wide;

-- Indices should have been dropped
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

-- Regenerate them
OPTIMIZE TABLE test_compact FINAL;
OPTIMIZE TABLE test_wide FINAL;

-- Check that indices are back
SELECT 'We are back #1';
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

-- Alter of unrelated columns should only drop indices if needed (compact format is dropped, but wide is kept)
ALTER TABLE test_compact UPDATE c = 7 WHERE c = 6;
SELECT 'COMPACT AFTER UPDATE', * from test_compact;
ALTER TABLE test_wide UPDATE c = 7 WHERE c = 6;
SELECT 'WIDE AFTER UPDATE', * from test_wide;

-- Indices should have been dropped
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

-- Regenerate them
OPTIMIZE TABLE test_compact FINAL;
OPTIMIZE TABLE test_wide FINAL;
-- Check that indices are back
SELECT 'We are back #2';
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';

-- Delete over any column should drop indices since it requires changes
ALTER TABLE test_compact DELETE WHERE c = 7;
SELECT 'COMPACT AFTER DELETE', * from test_compact;
ALTER TABLE test_wide DELETE WHERE c = 7;
SELECT 'WIDE AFTER UPDATE', * from test_wide;

-- Indices should have been dropped
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_compact' AND database = currentDatabase() AND name = 'idx_minmax';
SELECT table, name, 'Emtpy : ' || if(marks_bytes == 0, 'true', 'false') FROM system.data_skipping_indices WHERE table = 'test_wide' AND database = currentDatabase() AND name = 'idx_minmax';
