DROP TABLE IF EXISTS t_lwu_deletes_3 SYNC;

CREATE TABLE t_lwu_deletes_3 (id UInt64, dt Date, v1 UInt64, v2 String)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_deletes_3/', '1')
ORDER BY (id, dt)
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

SET apply_patch_parts = 1;
SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';

SYSTEM STOP MERGES t_lwu_deletes_3;

INSERT INTO t_lwu_deletes_3 SELECT number % 10000, toDate('2024-10-10'), 0, '' FROM numbers(100000);
INSERT INTO t_lwu_deletes_3 SELECT number % 10000, toDate('2024-11-11'), 0, '' FROM numbers(100000);
INSERT INTO t_lwu_deletes_3 SELECT number % 10000, toDate('2024-12-12'), 0, '' FROM numbers(100000);

UPDATE t_lwu_deletes_3 SET v1 = 42 WHERE id = 100;
UPDATE t_lwu_deletes_3 SET v1 = 42 WHERE id = 4000;
UPDATE t_lwu_deletes_3 SET v2 = 'foo' WHERE id >= 9500;

DELETE FROM t_lwu_deletes_3 WHERE id = 200;
DELETE FROM t_lwu_deletes_3 WHERE dt = toDate('2024-11-11') AND id >= 4000 AND id < 5000;
DELETE FROM t_lwu_deletes_3 WHERE dt = toDate('2024-11-11') AND id >= 3500 AND id < 4500;
DELETE FROM t_lwu_deletes_3 WHERE notEmpty(v2);

SELECT 'reference';
SELECT 300000 - 10 * 3 - 1500 * 10 - 500 * 10 * 3 , 42 * 10 * 5, 0;

SELECT 'before merge';
SELECT count(), sum(v1), sum(notEmpty(v2)) FROM t_lwu_deletes_3;

SELECT count(), uniqExact(partition_id), sum(rows)
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu_deletes_3' AND column = '_row_exists' AND active AND startsWith(name, 'patch');

SYSTEM START MERGES t_lwu_deletes_3;
OPTIMIZE TABLE t_lwu_deletes_3 PARTITION ID 'patch-f18f7271629a324b0d26b6ad0b83a6c2-all' FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT 'after merge patch';
SELECT count(), sum(v1), sum(notEmpty(v2)) FROM t_lwu_deletes_3;

SELECT count(), uniqExact(partition_id), sum(rows)
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu_deletes_3' AND column = '_row_exists' AND active AND startsWith(name, 'patch');

OPTIMIZE TABLE t_lwu_deletes_3 PARTITION ID 'all' FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT 'after merge main';
SELECT count(), sum(v1), sum(notEmpty(v2)) FROM t_lwu_deletes_3 SETTINGS apply_patch_parts = 0;
SELECT sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 't_lwu_deletes_3' AND NOT startsWith(name, 'patch') AND active;

DROP TABLE t_lwu_deletes_3 SYNC;
