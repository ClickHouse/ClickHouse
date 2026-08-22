DROP TABLE IF EXISTS t_shared SYNC;
SET enable_lightweight_update = 1;

CREATE TABLE t_shared (id UInt64, c1 UInt64, c2 String)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_shared/', '1')
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

INSERT INTO t_shared VALUES (1, 11, 'foo') (2, 22, 'bar') (3, 33, 'sss');

UPDATE t_shared SET c1 = 111, c2 = 'aaa' WHERE id = 1;
UPDATE t_shared SET c2 = 'aaa' WHERE id = 3;

SELECT * FROM t_shared ORDER BY id SETTINGS apply_patch_parts = 0;
SELECT * FROM t_shared ORDER BY id SETTINGS apply_patch_parts = 1;

SELECT * FROM t_shared PREWHERE c2 = 'aaa' AND c1 = 111;
SELECT * FROM t_shared WHERE c2 = 'aaa' AND c1 = 111;
SELECT * FROM t_shared PREWHERE c2 = 'aaa' WHERE c1 = 111;

DROP TABLE t_shared SYNC;
