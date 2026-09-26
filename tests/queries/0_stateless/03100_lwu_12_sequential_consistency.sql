-- Tags: replica

DROP TABLE IF EXISTS t_lwu_sequential_1 SYNC;
DROP TABLE IF EXISTS t_lwu_sequential_2 SYNC;

SET enable_lightweight_update = 1;

CREATE TABLE t_lwu_sequential_1 (id UInt64, s FixedString(3))
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_sequential/', '1')
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

CREATE TABLE t_lwu_sequential_2 (id UInt64, s FixedString(3))
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_sequential/', '2')
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

SET update_sequential_consistency = 1;
SET select_sequential_consistency = 0;

INSERT INTO t_lwu_sequential_1 VALUES (1, 'abc'), (2, 'def');

UPDATE t_lwu_sequential_2 SET s = 'foo' WHERE id = 1;

SET select_sequential_consistency = 1;
SYSTEM SYNC REPLICA t_lwu_sequential_1 LIGHTWEIGHT;

SELECT * FROM t_lwu_sequential_1 ORDER BY id SETTINGS apply_patch_parts = 0;
SELECT * FROM t_lwu_sequential_1 ORDER BY id SETTINGS apply_patch_parts = 1;
