DROP TABLE IF EXISTS t_lwu_patch_partition_ops SYNC;
DROP TABLE IF EXISTS t_lwu_patch_partition_ops_dst SYNC;
DROP TABLE IF EXISTS t_lwu_patch_partition_ops_dst_all SYNC;

SET enable_lightweight_update = 1;

CREATE TABLE t_lwu_patch_partition_ops
(
    id UInt64,
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY a
PARTITION BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE t_lwu_patch_partition_ops_dst AS t_lwu_patch_partition_ops
ENGINE = MergeTree
ORDER BY a
PARTITION BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE t_lwu_patch_partition_ops_dst_all AS t_lwu_patch_partition_ops
ENGINE = MergeTree
ORDER BY a
PARTITION BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_patch_partition_ops VALUES (1, 1, 1), (1, 2, 2), (2, 1, 1), (3, 1, 1);

UPDATE t_lwu_patch_partition_ops SET b = b + 10 WHERE id = 1;
UPDATE t_lwu_patch_partition_ops SET b = b + 20 WHERE id = 2;
UPDATE t_lwu_patch_partition_ops SET b = b + 30 WHERE id = 3;

ALTER TABLE t_lwu_patch_partition_ops_dst ATTACH PARTITION ID '1' FROM t_lwu_patch_partition_ops;
ALTER TABLE t_lwu_patch_partition_ops MOVE PARTITION ID '2' TO TABLE t_lwu_patch_partition_ops_dst;
ALTER TABLE t_lwu_patch_partition_ops_dst REPLACE PARTITION ID '3' FROM t_lwu_patch_partition_ops;

SELECT id, a, b FROM t_lwu_patch_partition_ops_dst ORDER BY id, a;

ALTER TABLE t_lwu_patch_partition_ops_dst_all ATTACH PARTITION ALL FROM t_lwu_patch_partition_ops;
SELECT id, a, b FROM t_lwu_patch_partition_ops_dst_all ORDER BY id, a;

ALTER TABLE t_lwu_patch_partition_ops DETACH PARTITION ID '1';
ALTER TABLE t_lwu_patch_partition_ops ATTACH PARTITION ID '1';

SELECT id, a, b FROM t_lwu_patch_partition_ops ORDER BY id, a;

DROP TABLE t_lwu_patch_partition_ops SYNC;
DROP TABLE t_lwu_patch_partition_ops_dst SYNC;
DROP TABLE t_lwu_patch_partition_ops_dst_all SYNC;
