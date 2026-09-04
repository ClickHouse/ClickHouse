-- A merge must decide what data survives from the current TTL expression evaluated over the rows,
-- not from the bounds stored in `ttl.txt`. `materialize_ttl_after_modify = 0` leaves a part whose
-- stored bounds are expired under the retired rule while the live rule keeps every row, which is
-- the state an upgrade also produces when the expression's evaluation semantics change.

DROP TABLE IF EXISTS t_ttl_rows;
DROP TABLE IF EXISTS t_ttl_rows_materialized;
DROP TABLE IF EXISTS t_ttl_vertical;
DROP TABLE IF EXISTS t_ttl_column_wide;
DROP TABLE IF EXISTS t_ttl_column_compact;
DROP TABLE IF EXISTS t_ttl_materialize;
DROP TABLE IF EXISTS t_ttl_still_deletes;
DROP TABLE IF EXISTS t_ttl_group_by;
DROP TABLE IF EXISTS t_ttl_patch_part;

SELECT '-- rows TTL, horizontal merge';

CREATE TABLE t_ttl_rows (ts DateTime, v UInt64) ENGINE = MergeTree ORDER BY tuple()
TTL ts + INTERVAL 1 SECOND SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_ttl_rows;
INSERT INTO t_ttl_rows VALUES (now() - INTERVAL 1 DAY, 42);
ALTER TABLE t_ttl_rows MODIFY TTL ts + INTERVAL 10 YEAR SETTINGS materialize_ttl_after_modify = 0;
SYSTEM START MERGES t_ttl_rows;
OPTIMIZE TABLE t_ttl_rows FINAL;
SELECT count(), sum(v) FROM t_ttl_rows;
-- The merge also rewrites the stored bounds, so the part is no longer a trap for the next merge.
SELECT delete_ttl_info_min > now() FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_rows' AND active;

SELECT '-- rows TTL, in-range control: the same DDL with the default materialize_ttl_after_modify';

-- The mutation this ALTER submits only runs while merges are started, so the part is protected with
-- `STOP TTL MERGES` rather than `STOP MERGES`: otherwise whichever of the two runs first decides the
-- result.
CREATE TABLE t_ttl_rows_materialized (ts DateTime, v UInt64) ENGINE = MergeTree ORDER BY tuple()
TTL ts + INTERVAL 1 SECOND SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP TTL MERGES t_ttl_rows_materialized;
INSERT INTO t_ttl_rows_materialized VALUES (now() - INTERVAL 1 DAY, 42);
ALTER TABLE t_ttl_rows_materialized MODIFY TTL ts + INTERVAL 10 YEAR
SETTINGS materialize_ttl_after_modify = 1, mutations_sync = 2;
SYSTEM START TTL MERGES t_ttl_rows_materialized;
OPTIMIZE TABLE t_ttl_rows_materialized FINAL;
SELECT count(), sum(v) FROM t_ttl_rows_materialized;

SELECT '-- rows TTL, vertical merge';

CREATE TABLE t_ttl_vertical (ts DateTime, a UInt64, b String) ENGINE = MergeTree ORDER BY tuple()
TTL ts + INTERVAL 1 SECOND
-- Every one of these is a condition `chooseMergeAlgorithm` tests, and the runner randomizes them:
-- a part below `min_bytes_for_full_part_storage` gets Packed storage, which is horizontal-only.
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         enable_vertical_merge_algorithm = 1, vertical_merge_optimize_ttl_delete = 1,
         vertical_merge_algorithm_min_rows_to_activate = 0,
         vertical_merge_algorithm_min_columns_to_activate = 1;
SYSTEM STOP MERGES t_ttl_vertical;
INSERT INTO t_ttl_vertical VALUES (now() - INTERVAL 1 DAY, 1, 'x');
INSERT INTO t_ttl_vertical VALUES (now() - INTERVAL 1 DAY, 2, 'y');
ALTER TABLE t_ttl_vertical MODIFY TTL ts + INTERVAL 10 YEAR SETTINGS materialize_ttl_after_modify = 0;
SYSTEM START MERGES t_ttl_vertical;
OPTIMIZE TABLE t_ttl_vertical FINAL;
SELECT count(), sum(a) FROM t_ttl_vertical;
-- Without this the case would silently be a second horizontal-merge test.
SYSTEM FLUSH LOGS part_log;
SELECT merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 't_ttl_vertical' AND event_type = 'MergeParts'
  AND length(merged_from) > 1
ORDER BY event_time DESC LIMIT 1;

SELECT '-- column TTL, wide part';

CREATE TABLE t_ttl_column_wide (ts DateTime, v UInt64 TTL ts + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_ttl_column_wide;
INSERT INTO t_ttl_column_wide VALUES (now() - INTERVAL 1 DAY, 7777);
ALTER TABLE t_ttl_column_wide MODIFY COLUMN v UInt64 TTL ts + INTERVAL 10 YEAR
SETTINGS materialize_ttl_after_modify = 0;
SYSTEM START MERGES t_ttl_column_wide;
OPTIMIZE TABLE t_ttl_column_wide FINAL;
SELECT count(), sum(v) FROM t_ttl_column_wide;

SELECT '-- column TTL, compact part';

CREATE TABLE t_ttl_column_compact (ts DateTime, v UInt64 TTL ts + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 1000000000;
SYSTEM STOP MERGES t_ttl_column_compact;
INSERT INTO t_ttl_column_compact VALUES (now() - INTERVAL 1 DAY, 7777);
ALTER TABLE t_ttl_column_compact MODIFY COLUMN v UInt64 TTL ts + INTERVAL 10 YEAR
SETTINGS materialize_ttl_after_modify = 0;
SYSTEM START MERGES t_ttl_column_compact;
OPTIMIZE TABLE t_ttl_column_compact FINAL;
SELECT count(), sum(v) FROM t_ttl_column_compact;

SELECT '-- MATERIALIZE TTL repairs a part instead of emptying it';

CREATE TABLE t_ttl_materialize (ts DateTime, v UInt64) ENGINE = MergeTree ORDER BY tuple()
TTL ts + INTERVAL 1 SECOND SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_ttl_materialize;
INSERT INTO t_ttl_materialize VALUES (now() - INTERVAL 1 DAY, 42);
ALTER TABLE t_ttl_materialize MODIFY TTL ts + INTERVAL 10 YEAR SETTINGS materialize_ttl_after_modify = 0;
SYSTEM START MERGES t_ttl_materialize;
ALTER TABLE t_ttl_materialize MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT count(), sum(v) FROM t_ttl_materialize;
SELECT delete_ttl_info_min > now() FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_materialize' AND active;

SELECT '-- genuinely expired rows are still deleted';

CREATE TABLE t_ttl_still_deletes (ts DateTime, v UInt64) ENGINE = MergeTree ORDER BY tuple()
TTL ts + INTERVAL 1 SECOND SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_ttl_still_deletes VALUES (now() - INTERVAL 1 DAY, 42);
OPTIMIZE TABLE t_ttl_still_deletes FINAL;
SELECT count() FROM t_ttl_still_deletes;

SELECT '-- GROUP BY TTL is not permanently disabled';

-- The map key of a GROUP BY rule is the formatted expression text, so changing the interval would
-- create a fresh key and force a full recalculation. Patching the column the expression reads keeps
-- the key and leaves the stored bounds expired while no row is.
CREATE TABLE t_ttl_group_by (k UInt64, ts DateTime, v UInt64) ENGINE = MergeTree ORDER BY k
TTL ts + INTERVAL 1 SECOND GROUP BY k SET v = max(v)
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_ttl_group_by;
INSERT INTO t_ttl_group_by VALUES (1, now() - INTERVAL 1 DAY, 10), (1, now() - INTERVAL 1 DAY, 20);
SET enable_lightweight_update = 1;
UPDATE t_ttl_group_by SET ts = toDateTime('2099-01-01 00:00:00') WHERE 1;
SYSTEM START MERGES t_ttl_group_by;
OPTIMIZE TABLE t_ttl_group_by FINAL;
SELECT count(), sum(v) FROM t_ttl_group_by;
-- {0, 0} here would mean the rule was persisted as finished and the rollup can never run again.
-- The patch part carries no GROUP BY record, so it is not one of the parts under test.
SELECT arrayMap(x -> x > now(), group_by_ttl_info.min) FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_group_by' AND active
  AND notEmpty(group_by_ttl_info.min);

SELECT '-- a patch part that un-expires rows is honoured';

CREATE TABLE t_ttl_patch_part (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id
TTL ts + INTERVAL 1 SECOND
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_ttl_patch_part;
INSERT INTO t_ttl_patch_part VALUES (1, now() - INTERVAL 1 DAY), (2, now() - INTERVAL 1 DAY), (3, now() - INTERVAL 1 DAY);
UPDATE t_ttl_patch_part SET ts = toDateTime('2099-01-01 00:00:00') WHERE id = 1;
SYSTEM START MERGES t_ttl_patch_part;
OPTIMIZE TABLE t_ttl_patch_part FINAL;
-- Rows 2 and 3 are genuinely expired and must still go.
SELECT count(), min(id) FROM t_ttl_patch_part;

DROP TABLE t_ttl_rows;
DROP TABLE t_ttl_rows_materialized;
DROP TABLE t_ttl_vertical;
DROP TABLE t_ttl_column_wide;
DROP TABLE t_ttl_column_compact;
DROP TABLE t_ttl_materialize;
DROP TABLE t_ttl_still_deletes;
DROP TABLE t_ttl_group_by;
DROP TABLE t_ttl_patch_part;
