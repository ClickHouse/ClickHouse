DROP TABLE IF EXISTS t_lwu_sequence;
SET enable_lightweight_update = 1;

CREATE TABLE t_lwu_sequence (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 1,
    shared_merge_tree_disable_merges_and_mutations_assignment = 1;

INSERT INTO t_lwu_sequence SELECT number, number FROM numbers(10);

UPDATE t_lwu_sequence SET b = 500 WHERE a = 5;
UPDATE t_lwu_sequence SET b = 501 WHERE a = 5;
UPDATE t_lwu_sequence SET b = 502 WHERE a = 5;
UPDATE t_lwu_sequence SET b = 503 WHERE a = 5;

SELECT b FROM t_lwu_sequence WHERE a = 5 SETTINGS apply_patch_parts = 0;
SELECT b FROM t_lwu_sequence WHERE a = 5 SETTINGS apply_patch_parts = 1;

SELECT count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 't_lwu_sequence' AND startsWith(name, 'patch') AND active;

OPTIMIZE TABLE t_lwu_sequence PARTITION ID 'patch-d9dff7d4cface4172f96b0bae7cb2e83-all' FINAL;

SELECT b FROM t_lwu_sequence WHERE a = 5 SETTINGS apply_patch_parts = 0;
SELECT b FROM t_lwu_sequence WHERE a = 5 SETTINGS apply_patch_parts = 1;

SELECT count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 't_lwu_sequence' AND startsWith(name, 'patch') AND active;

OPTIMIZE TABLE t_lwu_sequence PARTITION ID 'all' FINAL;

SELECT b FROM t_lwu_sequence WHERE a = 5 SETTINGS apply_patch_parts = 0;
SELECT b FROM t_lwu_sequence WHERE a = 5 SETTINGS apply_patch_parts = 1;

DROP TABLE t_lwu_sequence;
