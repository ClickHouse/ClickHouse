-- Clone of 03100_lwu_deletes_3 with the legacy patch part format (`patch_parts_version = 'v1'`).
DROP TABLE IF EXISTS t_lwu_deletes_3 SYNC;

CREATE TABLE t_lwu_deletes_3 (id UInt64, dt Date, v1 UInt64, v2 String)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_deletes_3/', '1')
ORDER BY (id, dt)
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    patch_parts_version = 'v1';

SET apply_patch_parts = 1;
SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';

SYSTEM STOP MERGES t_lwu_deletes_3;

INSERT INTO t_lwu_deletes_3 SELECT number % 2000, toDate('2024-10-10'), 0, '' FROM numbers(20000);
INSERT INTO t_lwu_deletes_3 SELECT number % 2000, toDate('2024-11-11'), 0, '' FROM numbers(20000);
INSERT INTO t_lwu_deletes_3 SELECT number % 2000, toDate('2024-12-12'), 0, '' FROM numbers(20000);

UPDATE t_lwu_deletes_3 SET v1 = 42 WHERE id = 20;
UPDATE t_lwu_deletes_3 SET v1 = 42 WHERE id = 800;
UPDATE t_lwu_deletes_3 SET v2 = 'foo' WHERE id >= 1900;

DELETE FROM t_lwu_deletes_3 WHERE id = 40;
DELETE FROM t_lwu_deletes_3 WHERE dt = toDate('2024-11-11') AND id >= 800 AND id < 1000;
DELETE FROM t_lwu_deletes_3 WHERE dt = toDate('2024-11-11') AND id >= 700 AND id < 900;
DELETE FROM t_lwu_deletes_3 WHERE notEmpty(v2);

SELECT 'reference';
SELECT 60000 - 10 * 3 - 300 * 10 - 100 * 10 * 3 , 42 * 10 * 5, 0;

SELECT 'before merge';
SELECT count(), sum(v1), sum(notEmpty(v2)) FROM t_lwu_deletes_3;

SELECT count(), uniqExact(partition_id), sum(rows)
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu_deletes_3' AND column = '_row_exists' AND active AND startsWith(name, 'patch');

SYSTEM START MERGES t_lwu_deletes_3;
SYSTEM SYNC REPLICA t_lwu_deletes_3 PULL;
OPTIMIZE TABLE t_lwu_deletes_3 PARTITION ID 'patch-071ac791eb95e15357d57becc6ae6c7b-all' FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT 'after merge patch';
SELECT count(), sum(v1), sum(notEmpty(v2)) FROM t_lwu_deletes_3;

SELECT count(), uniqExact(partition_id), sum(rows)
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu_deletes_3' AND column = '_row_exists' AND active AND startsWith(name, 'patch');

SYSTEM SYNC REPLICA t_lwu_deletes_3 PULL;
OPTIMIZE TABLE t_lwu_deletes_3 PARTITION ID 'all' FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT 'after merge main';
SELECT count(), sum(v1), sum(notEmpty(v2)) FROM t_lwu_deletes_3 SETTINGS apply_patch_parts = 0;
SELECT sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 't_lwu_deletes_3' AND NOT startsWith(name, 'patch') AND active;

DROP TABLE t_lwu_deletes_3 SYNC;
