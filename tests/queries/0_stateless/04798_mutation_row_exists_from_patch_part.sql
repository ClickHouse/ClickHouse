-- Tags: no-replicated-database, long
-- long: one run of this file goes past the flaky check's 180s per-run budget under ASan
-- with S3 storage and metadata in Keeper, where every statement pays an object-storage
-- round trip. Untagged, that budget fails the check outright rather than reporting a flake.
-- no-replicated-database: fails due to additional shard.

SET insert_keeper_fault_injection_probability = 0.0;
SET enable_lightweight_update = 1;
SET mutations_sync = 2, alter_sync = 2, lightweight_deletes_sync = 2;

-- A lightweight delete in patch mode supplies _row_exists from a patch part rather than from the
-- source part's own columns. A later mutation that rewrites all columns applies that mask on read
-- and physically drops the masked rows, so its result must not be declared to carry the mask.

SELECT '-- lightweight_update: heavyweight DELETE after a patch-mode lightweight DELETE';

DROP TABLE IF EXISTS t_lwu SYNC;
CREATE TABLE t_lwu (k UInt64, v UInt64) ENGINE = MergeTree PARTITION BY (k % 4) ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_lwu SELECT number, number FROM numbers(24);

DELETE FROM t_lwu WHERE k % 3 = 0 SETTINGS lightweight_delete_mode = 'lightweight_update';
ALTER TABLE t_lwu UPDATE v = v + 1 WHERE k = 1;
DELETE FROM t_lwu WHERE k = 1 SETTINGS lightweight_delete_mode = 'lightweight_update';
ALTER TABLE t_lwu DELETE WHERE k = 2;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_lwu' AND NOT is_done;
SELECT k, v FROM t_lwu ORDER BY k;
SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu' AND active
  AND column = '_row_exists' AND name NOT LIKE 'patch%' AND name LIKE '2\_%';

SELECT '-- alter_update mode produces the same rows (oracle)';

DROP TABLE IF EXISTS t_alter SYNC;
CREATE TABLE t_alter (k UInt64, v UInt64) ENGINE = MergeTree PARTITION BY (k % 4) ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_alter SELECT number, number FROM numbers(24);

DELETE FROM t_alter WHERE k % 3 = 0 SETTINGS lightweight_delete_mode = 'alter_update';
ALTER TABLE t_alter UPDATE v = v + 1 WHERE k = 1;
DELETE FROM t_alter WHERE k = 1 SETTINGS lightweight_delete_mode = 'alter_update';
ALTER TABLE t_alter DELETE WHERE k = 2;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_alter' AND NOT is_done;
SELECT k, v FROM t_alter ORDER BY k;

SELECT '-- both modes agree';
SELECT (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lwu ORDER BY k))
     = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_alter ORDER BY k));

SELECT '-- lightweight_update_force';

DROP TABLE IF EXISTS t_force SYNC;
CREATE TABLE t_force (k UInt64, v UInt64) ENGINE = MergeTree PARTITION BY (k % 4) ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_force SELECT number, number FROM numbers(24);

DELETE FROM t_force WHERE k % 3 = 0 SETTINGS lightweight_delete_mode = 'lightweight_update_force';
ALTER TABLE t_force UPDATE v = v + 1 WHERE k = 1;
DELETE FROM t_force WHERE k = 1 SETTINGS lightweight_delete_mode = 'lightweight_update_force';
ALTER TABLE t_force DELETE WHERE k = 2;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_force' AND NOT is_done;
SELECT k, v FROM t_force ORDER BY k;

SELECT '-- APPLY DELETED MASK and REWRITE PARTS reach the same code path';

DROP TABLE IF EXISTS t_mask SYNC;
CREATE TABLE t_mask (k UInt64, v UInt64) ENGINE = MergeTree PARTITION BY (k % 4) ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_mask SELECT number, number FROM numbers(24);

DELETE FROM t_mask WHERE k % 3 = 0 SETTINGS lightweight_delete_mode = 'lightweight_update';
ALTER TABLE t_mask UPDATE v = v + 1 WHERE k = 1;
DELETE FROM t_mask WHERE k = 1 SETTINGS lightweight_delete_mode = 'lightweight_update';
ALTER TABLE t_mask APPLY DELETED MASK;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mask' AND NOT is_done;
SELECT k, v FROM t_mask ORDER BY k;
SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_mask' AND active
  AND column = '_row_exists' AND name NOT LIKE 'patch%';

DROP TABLE IF EXISTS t_rewrite SYNC;
CREATE TABLE t_rewrite (k UInt64, v UInt64) ENGINE = MergeTree PARTITION BY (k % 4) ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_rewrite SELECT number, number FROM numbers(24);

DELETE FROM t_rewrite WHERE k % 3 = 0 SETTINGS lightweight_delete_mode = 'lightweight_update';
ALTER TABLE t_rewrite UPDATE v = v + 1 WHERE k = 1;
DELETE FROM t_rewrite WHERE k = 1 SETTINGS lightweight_delete_mode = 'lightweight_update';
ALTER TABLE t_rewrite REWRITE PARTS;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_rewrite' AND NOT is_done;
SELECT k, v FROM t_rewrite ORDER BY k;

SELECT '-- a partial update keeps the mask, so masked rows stay hidden';

DROP TABLE IF EXISTS t_partial SYNC;
CREATE TABLE t_partial (k UInt64, v UInt64) ENGINE = MergeTree PARTITION BY (k % 4) ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_partial SELECT number, number FROM numbers(24);

DELETE FROM t_partial WHERE k % 3 = 0 SETTINGS lightweight_delete_mode = 'lightweight_update';
ALTER TABLE t_partial UPDATE v = v + 100 WHERE k % 2 = 1;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_partial' AND NOT is_done;
SELECT k, v FROM t_partial ORDER BY k;
SELECT count() > 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_partial' AND active
  AND column = '_row_exists' AND name NOT LIKE 'patch%';

SELECT '-- a heavyweight DELETE matching no rows keeps the pending patch';

DROP TABLE IF EXISTS t_noop SYNC;
CREATE TABLE t_noop (k UInt64, v UInt64) ENGINE = MergeTree PARTITION BY (k % 4) ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_noop SELECT number, number FROM numbers(24);

DELETE FROM t_noop WHERE k % 3 = 0 SETTINGS lightweight_delete_mode = 'lightweight_update';
ALTER TABLE t_noop DELETE WHERE 1 = 2;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_noop' AND NOT is_done;
SELECT count(), countIf(k % 3 = 0) FROM t_noop;

SELECT '-- compact parts';

DROP TABLE IF EXISTS t_compact SYNC;
CREATE TABLE t_compact (k UInt64, v UInt64) ENGINE = MergeTree PARTITION BY (k % 4) ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000,
         min_bytes_for_full_part_storage = 0;
INSERT INTO t_compact SELECT number, number FROM numbers(24);

DELETE FROM t_compact WHERE k % 3 = 0 SETTINGS lightweight_delete_mode = 'lightweight_update';
ALTER TABLE t_compact UPDATE v = v + 1 WHERE k = 1;
DELETE FROM t_compact WHERE k = 1 SETTINGS lightweight_delete_mode = 'lightweight_update';
ALTER TABLE t_compact DELETE WHERE k = 2;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_compact' AND NOT is_done;
SELECT k, v FROM t_compact ORDER BY k;

SELECT '-- with a projection';

DROP TABLE IF EXISTS t_proj SYNC;
CREATE TABLE t_proj (k UInt64, v UInt64, PROJECTION p (SELECT v, count() GROUP BY v))
ENGINE = MergeTree PARTITION BY (k % 4) ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         deduplicate_merge_projection_mode = 'rebuild', lightweight_mutation_projection_mode = 'rebuild';
INSERT INTO t_proj SELECT number, number FROM numbers(24);

DELETE FROM t_proj WHERE k % 3 = 0 SETTINGS lightweight_delete_mode = 'lightweight_update';
ALTER TABLE t_proj UPDATE v = v + 1 WHERE k = 1;
DELETE FROM t_proj WHERE k = 1 SETTINGS lightweight_delete_mode = 'lightweight_update';
ALTER TABLE t_proj DELETE WHERE k = 2;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_proj' AND NOT is_done;
SELECT k, v FROM t_proj ORDER BY k;

SELECT '-- ReplicatedMergeTree';

DROP TABLE IF EXISTS t_repl SYNC;
CREATE TABLE t_repl (k UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_repl/', '1') PARTITION BY (k % 4) ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_repl SELECT number, number FROM numbers(24);

DELETE FROM t_repl WHERE k % 3 = 0 SETTINGS lightweight_delete_mode = 'lightweight_update';
ALTER TABLE t_repl UPDATE v = v + 1 WHERE k = 1;
DELETE FROM t_repl WHERE k = 1 SETTINGS lightweight_delete_mode = 'lightweight_update';
ALTER TABLE t_repl DELETE WHERE k = 2;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_repl' AND NOT is_done;
SELECT k, v FROM t_repl ORDER BY k;

DROP TABLE t_lwu SYNC;
DROP TABLE t_alter SYNC;
DROP TABLE t_force SYNC;
DROP TABLE t_mask SYNC;
DROP TABLE t_rewrite SYNC;
DROP TABLE t_partial SYNC;
DROP TABLE t_noop SYNC;
DROP TABLE t_compact SYNC;
DROP TABLE t_proj SYNC;
DROP TABLE t_repl SYNC;
