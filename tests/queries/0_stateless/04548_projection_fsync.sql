-- Tags: no-object-storage
-- no-object-storage: object storage does not fsync files.

-- Regression test for issue #111318: projection parts must be fsynced together with the part.
-- A fully synced part fsyncs several more files when it has a projection, so the durability of
-- the projection is checked by comparing `ProfileEvents['FileSync']` of a table with a projection
-- against an identical table without one (read back from `system.query_log`), for both the `INSERT`
-- (`fsync_after_insert`) and the merge (`OPTIMIZE`, gated by the `*_to_fsync_after_merge`
-- thresholds) paths. `max_bytes_to_merge_at_max_space_in_pool = 1` disables background merges so
-- the only merger is the `OPTIMIZE FINAL` whose fsyncs are attributed to it.

DROP TABLE IF EXISTS t_proj;
DROP TABLE IF EXISTS t_plain;

CREATE TABLE t_proj (id UInt64, key String, v UInt64,
                     PROJECTION pr (SELECT key, sum(v) GROUP BY key))
ENGINE = MergeTree ORDER BY id
SETTINGS fsync_after_insert = 1, fsync_part_directory = 1,
         min_rows_to_fsync_after_merge = 1, min_compressed_bytes_to_fsync_after_merge = 1,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1;

CREATE TABLE t_plain (id UInt64, key String, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS fsync_after_insert = 1, fsync_part_directory = 1,
         min_rows_to_fsync_after_merge = 1, min_compressed_bytes_to_fsync_after_merge = 1,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1;

-- `INSERT` path: `fsync_after_insert = 1` must fsync the projection files too.
INSERT INTO t_proj SELECT number, concat('k', toString(number % 7)), number FROM numbers(5000);
INSERT INTO t_proj SELECT number + 5000, concat('k', toString(number % 7)), number FROM numbers(5000);
INSERT INTO t_plain SELECT number, concat('k', toString(number % 7)), number FROM numbers(5000);
INSERT INTO t_plain SELECT number + 5000, concat('k', toString(number % 7)), number FROM numbers(5000);

-- Merge path: the projection parts are merged during the merge and must be fsynced too.
OPTIMIZE TABLE t_proj FINAL SETTINGS optimize_throw_if_noop = 1, alter_sync = 2;
OPTIMIZE TABLE t_plain FINAL SETTINGS optimize_throw_if_noop = 1, alter_sync = 2;

SYSTEM FLUSH LOGS query_log;

-- The projection `INSERT` must fsync strictly more files than the identical plain `INSERT`.
SELECT 'insert projection adds file syncs',
    (SELECT max(ProfileEvents['FileSync']) FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Insert'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_proj%' AND type = 'QueryFinish')
    >
    (SELECT max(ProfileEvents['FileSync']) FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Insert'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_plain%' AND type = 'QueryFinish');

-- The projection merge must fsync strictly more files than the identical plain merge.
SELECT 'merge projection adds file syncs',
    (SELECT ProfileEvents['FileSync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Optimize'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_proj%' AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1)
    >
    (SELECT ProfileEvents['FileSync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Optimize'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_plain%' AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1);

-- The part and its projection must be complete and readable.
SELECT 'rows', count() FROM t_proj;
SELECT sum(v) FROM t_proj GROUP BY key ORDER BY key SETTINGS force_optimize_projection = 1 FORMAT Null;
SELECT 'check';
CHECK TABLE t_proj SETTINGS check_query_single_value_result = 1;

DROP TABLE t_proj;
DROP TABLE t_plain;

-- `MATERIALIZE PROJECTION` (mutation) path: its fsyncs run on the IO thread pool and are not
-- attributed to the `ALTER` in `system.query_log`, so a fsync delta is not observable here.
-- Instead exercise the mutation entry point end to end and check the projection is readable.
CREATE TABLE t_mat (id UInt64, key String, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS fsync_after_insert = 1, fsync_part_directory = 1,
         min_rows_to_fsync_after_merge = 1, min_compressed_bytes_to_fsync_after_merge = 1,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_mat SELECT number, concat('k', toString(number % 7)), number FROM numbers(10000);
ALTER TABLE t_mat ADD PROJECTION pr (SELECT key, sum(v) GROUP BY key);
ALTER TABLE t_mat MATERIALIZE PROJECTION pr SETTINGS mutations_sync = 2;

SELECT 'materialize rows', count() FROM t_mat;
SELECT sum(v) FROM t_mat GROUP BY key ORDER BY key SETTINGS force_optimize_projection = 1 FORMAT Null;
SELECT 'materialize check';
CHECK TABLE t_mat SETTINGS check_query_single_value_result = 1;

DROP TABLE t_mat;
