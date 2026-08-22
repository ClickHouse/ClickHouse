SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';
SET apply_patch_parts = 1;

DROP TABLE IF EXISTS t_lwu_rev SYNC;
DROP TABLE IF EXISTS t_lwu_rev_multi SYNC;
DROP TABLE IF EXISTS t_lwu_asc SYNC;
DROP TABLE IF EXISTS t_lwu_rev_expr SYNC;
DROP TABLE IF EXISTS t_lwu_empty SYNC;
DROP TABLE IF EXISTS t_lwu_rev_replacing SYNC;
DROP TABLE IF EXISTS t_lwu_rev_merge SYNC;
DROP TABLE IF EXISTS t_lwu_rev_v1 SYNC;

SELECT '-- single reversed key column';

CREATE TABLE t_lwu_rev (c0 UInt64, c1 String) ENGINE = MergeTree ORDER BY c0 DESC
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_rev SELECT number, 'v' || toString(number) FROM numbers(6);

DELETE FROM t_lwu_rev WHERE c0 % 2 = 0;
SELECT c0, c1 FROM t_lwu_rev ORDER BY c0;

UPDATE t_lwu_rev SET c1 = 'x' WHERE c0 = 3;
SELECT c0, c1 FROM t_lwu_rev ORDER BY c0;

SELECT '-- mixed directions in a multi-column key';

CREATE TABLE t_lwu_rev_multi (c0 UInt64, c1 UInt64, c2 UInt64, c3 String)
ENGINE = MergeTree ORDER BY (c0 DESC, c1, c2)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_rev_multi SELECT number % 3, number, number, 'v' || toString(number) FROM numbers(6);

DELETE FROM t_lwu_rev_multi WHERE c1 % 2 = 0;
SELECT c0, c1, c2, c3 FROM t_lwu_rev_multi ORDER BY c1;

UPDATE t_lwu_rev_multi SET c3 = 'x' WHERE c1 = 3;
SELECT c0, c1, c2, c3 FROM t_lwu_rev_multi ORDER BY c1;

SELECT '-- all-ascending key';

CREATE TABLE t_lwu_asc (c0 UInt64, c1 String) ENGINE = MergeTree ORDER BY (c0, c1)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_asc SELECT number, 'v' || toString(number) FROM numbers(6);

DELETE FROM t_lwu_asc WHERE c0 % 2 = 0;
SELECT c0, c1 FROM t_lwu_asc ORDER BY c0;

SELECT '-- reversed key with a non-identifier column';

CREATE TABLE t_lwu_rev_expr (c0 UInt64, c1 UInt64, c2 String)
ENGINE = MergeTree ORDER BY (c0 DESC, c1 + 1)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_rev_expr SELECT number, number, 'v' || toString(number) FROM numbers(6);

DELETE FROM t_lwu_rev_expr WHERE c0 % 2 = 0;
SELECT c0, c1, c2 FROM t_lwu_rev_expr ORDER BY c0;

UPDATE t_lwu_rev_expr SET c2 = 'x' WHERE c0 = 3;
SELECT c0, c1, c2 FROM t_lwu_rev_expr ORDER BY c0;

SELECT '-- empty sorting key';

CREATE TABLE t_lwu_empty (c0 UInt64, c1 String) ENGINE = MergeTree ORDER BY ()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_empty SELECT number, 'v' || toString(number) FROM numbers(6);

DELETE FROM t_lwu_empty WHERE c0 % 2 = 0;
SELECT c0, c1 FROM t_lwu_empty ORDER BY c0;

SELECT '-- reversed key with ReplacingMergeTree';

CREATE TABLE t_lwu_rev_replacing (c0 UInt64, c1 UInt64, c2 String)
ENGINE = ReplacingMergeTree(c1) ORDER BY c0 DESC
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_rev_replacing SELECT number, number, 'v' || toString(number) FROM numbers(6);

DELETE FROM t_lwu_rev_replacing WHERE c0 % 2 = 0;
SELECT c0, c1, c2 FROM t_lwu_rev_replacing ORDER BY c0;

UPDATE t_lwu_rev_replacing SET c2 = 'x' WHERE c0 = 3;
SELECT c0, c1, c2 FROM t_lwu_rev_replacing ORDER BY c0;

SELECT '-- two patches on a reversed key, then applied on merge';

CREATE TABLE t_lwu_rev_merge (c0 UInt64, c1 String, c2 String) ENGINE = MergeTree ORDER BY c0 DESC
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_rev_merge SELECT number, 'a' || toString(number), 'b' || toString(number) FROM numbers(6);

UPDATE t_lwu_rev_merge SET c1 = 'p1' WHERE c0 = 1;
UPDATE t_lwu_rev_merge SET c2 = 'p2' WHERE c0 = 3;
SELECT c0, c1, c2 FROM t_lwu_rev_merge ORDER BY c0;

OPTIMIZE TABLE t_lwu_rev_merge FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT c0, c1, c2 FROM t_lwu_rev_merge ORDER BY c0;

SELECT '-- reversed key with v1 patch parts';

CREATE TABLE t_lwu_rev_v1 (c0 UInt64, c1 String) ENGINE = MergeTree ORDER BY c0 DESC
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v1';

INSERT INTO t_lwu_rev_v1 SELECT number, 'v' || toString(number) FROM numbers(6);

DELETE FROM t_lwu_rev_v1 WHERE c0 % 2 = 0;
SELECT c0, c1 FROM t_lwu_rev_v1 ORDER BY c0;

UPDATE t_lwu_rev_v1 SET c1 = 'x' WHERE c0 = 3;
SELECT c0, c1 FROM t_lwu_rev_v1 ORDER BY c0;

DROP TABLE t_lwu_rev SYNC;
DROP TABLE t_lwu_rev_multi SYNC;
DROP TABLE t_lwu_asc SYNC;
DROP TABLE t_lwu_rev_expr SYNC;
DROP TABLE t_lwu_empty SYNC;
DROP TABLE t_lwu_rev_replacing SYNC;
DROP TABLE t_lwu_rev_merge SYNC;
DROP TABLE t_lwu_rev_v1 SYNC;
