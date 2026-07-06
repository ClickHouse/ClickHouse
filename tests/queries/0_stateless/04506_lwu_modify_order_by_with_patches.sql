DROP TABLE IF EXISTS t_lwu_modify_order_by SYNC;

CREATE TABLE t_lwu_modify_order_by (a UInt64, b UInt64, v String)
ENGINE = MergeTree
PRIMARY KEY a
ORDER BY (a, b)
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    patch_parts_version = 'v2';

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

INSERT INTO t_lwu_modify_order_by SELECT number, number, 'foo' FROM numbers(1000);

UPDATE t_lwu_modify_order_by SET v = 'bar' WHERE a < 100;

SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_modify_order_by' AND active AND startsWith(name, 'patch');

-- Shrinking the sorting key is not allowed while v2 patch parts exist.
ALTER TABLE t_lwu_modify_order_by MODIFY ORDER BY a; -- { serverError BAD_ARGUMENTS }

-- Replacing a sort-key expression is not allowed either.
ALTER TABLE t_lwu_modify_order_by ADD COLUMN c UInt64, MODIFY ORDER BY (a, c); -- { serverError BAD_ARGUMENTS }

-- Extending the sorting key keeps the persisted prefix intact and is allowed.
ALTER TABLE t_lwu_modify_order_by ADD COLUMN d UInt64, MODIFY ORDER BY (a, b, d);

SELECT count() FROM t_lwu_modify_order_by WHERE v = 'bar';

DROP TABLE t_lwu_modify_order_by SYNC;

-- v1 patch parts do not depend on the sorting key and must not block the ALTER.
CREATE TABLE t_lwu_modify_order_by_v1 (a UInt64, b UInt64, v String)
ENGINE = MergeTree
PRIMARY KEY a
ORDER BY (a, b)
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    patch_parts_version = 'v1';

INSERT INTO t_lwu_modify_order_by_v1 SELECT number, number, 'foo' FROM numbers(1000);

UPDATE t_lwu_modify_order_by_v1 SET v = 'bar' WHERE a < 100;

SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_modify_order_by_v1' AND active AND startsWith(name, 'patch');

ALTER TABLE t_lwu_modify_order_by_v1 MODIFY ORDER BY a;

SELECT count() FROM t_lwu_modify_order_by_v1 WHERE v = 'bar';

DROP TABLE t_lwu_modify_order_by_v1 SYNC;
