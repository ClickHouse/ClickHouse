DROP TABLE IF EXISTS t_lwu_drop_column;

CREATE TABLE t_lwu_drop_column
(
    id UInt64,
    s String,
)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_drop_column (id, s) VALUES (1, 'hello');

UPDATE t_lwu_drop_column SET s = 'update' WHERE id = 1;

SELECT * FROM t_lwu_drop_column ORDER BY id;

SET mutations_sync = 2;
ALTER TABLE t_lwu_drop_column DROP COLUMN s;
SELECT * FROM t_lwu_drop_column ORDER BY id;

DROP TABLE t_lwu_drop_column;
