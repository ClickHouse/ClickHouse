DROP TABLE IF EXISTS t_lwu_on_fly SYNC;
SET enable_lightweight_update = 1;

CREATE TABLE t_lwu_on_fly (id UInt64, a UInt64, b UInt64, c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_on_fly', '1') ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES t_lwu_on_fly;

INSERT INTO t_lwu_on_fly (id) VALUES (1) (2) (3);

SET apply_patch_parts = 1;
SET apply_mutations_on_fly = 1;

UPDATE t_lwu_on_fly SET a = 2 WHERE id = 2;

ALTER TABLE t_lwu_on_fly UPDATE b = 20 WHERE a = 2 SETTINGS mutations_sync = 0;

UPDATE t_lwu_on_fly SET c = 200 WHERE b = 20;

SELECT * FROM t_lwu_on_fly ORDER BY id;

DROP TABLE t_lwu_on_fly SYNC;
