DROP TABLE IF EXISTS t_lwu_replacing;

CREATE TABLE t_lwu_replacing (id UInt64, value String, timestamp DateTime)
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES t_lwu_replacing;

INSERT INTO t_lwu_replacing SELECT number, 'v' || toString(number), now() FROM numbers(10);
INSERT INTO t_lwu_replacing SELECT number, 'v' || toString(number), now() FROM numbers(5, 10);

SET apply_patch_parts = 1;
SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';

DELETE FROM t_lwu_replacing WHERE id % 2 = 0;
SELECT id, value FROM t_lwu_replacing FINAL ORDER BY id;
