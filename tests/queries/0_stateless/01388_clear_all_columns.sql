DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test (x) VALUES (1), (2), (3);
ALTER TABLE test CLEAR COLUMN x; --{serverError BAD_ARGUMENTS}
DROP TABLE test;

DROP TABLE IF EXISTS test;

CREATE TABLE test (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO test (x, y) VALUES (1, 1), (2, 2), (3, 3);

ALTER TABLE test CLEAR COLUMN x;

ALTER TABLE test CLEAR COLUMN x IN PARTITION ''; --{serverError INVALID_PARTITION_VALUE}
ALTER TABLE test CLEAR COLUMN x IN PARTITION 'asdasd'; --{serverError INVALID_PARTITION_VALUE}
ALTER TABLE test CLEAR COLUMN x IN PARTITION '123'; --{serverError INVALID_PARTITION_VALUE}

ALTER TABLE test CLEAR COLUMN y; --{serverError BAD_ARGUMENTS}

ALTER TABLE test ADD COLUMN z String DEFAULT 'Hello';

-- y is only real column in table
ALTER TABLE test CLEAR COLUMN y; --{serverError BAD_ARGUMENTS}
ALTER TABLE test CLEAR COLUMN x;
ALTER TABLE test CLEAR COLUMN z;

INSERT INTO test (x, y, z) VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c');

ALTER TABLE test CLEAR COLUMN z;
ALTER TABLE test CLEAR COLUMN x;

SELECT * FROM test ORDER BY y;

DROP TABLE IF EXISTS test;

-- A lightweight delete leaves `_row_exists` in the part, so dropping every table column the part
-- has still leaves the part with a column.
CREATE TABLE test (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO test (x, y) VALUES (1, 1), (2, 2);

DELETE FROM test WHERE x = 1;

ALTER TABLE test ADD COLUMN z String DEFAULT 'Hello';
ALTER TABLE test DROP COLUMN x, DROP COLUMN y SETTINGS mutations_sync = 2;

SELECT arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test' AND active;

SELECT * FROM test ORDER BY z;

DROP TABLE test;
