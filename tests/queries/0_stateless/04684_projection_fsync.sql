-- Tags: no-object-storage
-- no-object-storage: object storage does not fsync files.

-- Regression test for issue #111318: projection parts must be fsynced together with the part.
-- A fully synced part fsyncs several more files when it has a projection, so the durability of
-- the projection is checked by comparing `ProfileEvents['FileSync']` of a table with a projection
-- against an identical table without one (read back from `system.query_log`), for both the `INSERT`
-- (`fsync_after_insert`) and the merge (`OPTIMIZE`, gated by the `*_to_fsync_after_merge`
-- thresholds) paths. `max_bytes_to_merge_at_max_space_in_pool = 1` disables background merges so
-- the only merger is the `OPTIMIZE FINAL` whose fsyncs are attributed to it.
--
-- The merge thresholds are deliberately set ABOVE the projection part size and BELOW the parent
-- part size: the aggregate projection holds one row per `key`, so the projection sub-merge sees
-- far fewer rows than the parent. With a threshold small enough for the projection to clear it on
-- its own, the sub-merge decides to sync by itself and the assertion cannot observe whether the
-- parent's decision is propagated. `min_compressed_bytes_to_fsync_after_merge = 0` disables the
-- byte arm for the same reason (`needSyncPart` is an OR of the two arms).
--
-- `min_bytes_for_full_part_storage = 0` keeps the parts in `Full` storage. A `Packed` part is a
-- single blob with no per-file syncs to count, so the deltas below would read 0 even for a
-- correctly synced projection. That setting is randomized in CI, so it must be pinned here.

DROP TABLE IF EXISTS t_proj;
DROP TABLE IF EXISTS t_plain;

CREATE TABLE t_proj (id UInt64, key String, v UInt64,
                     PROJECTION pr (SELECT key, sum(v) GROUP BY key))
ENGINE = MergeTree ORDER BY id
SETTINGS fsync_after_insert = 1, fsync_part_directory = 1,
         min_rows_to_fsync_after_merge = 1000, min_compressed_bytes_to_fsync_after_merge = 0,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1;

CREATE TABLE t_plain (id UInt64, key String, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS fsync_after_insert = 1, fsync_part_directory = 1,
         min_rows_to_fsync_after_merge = 1000, min_compressed_bytes_to_fsync_after_merge = 0,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
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

-- Syncing the files inside `<projection>.proj` does not make the directory's own entries durable,
-- so the projection directory must be fsynced too. `DirectorySync` counts directory fsyncs only
-- (`LocalDirectorySyncGuard` is its sole source), so the projection table must report strictly
-- more of them than the identical plain table, which fsyncs only its own part directories.
SELECT 'insert projection adds directory syncs',
    (SELECT max(ProfileEvents['DirectorySync']) FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Insert'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_proj%' AND type = 'QueryFinish')
    >
    (SELECT max(ProfileEvents['DirectorySync']) FROM system.query_log
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

-- `MATERIALIZE PROJECTION` (mutation) path. Its fsyncs run on the IO thread pool and are not
-- attributed to the `ALTER` in `system.query_log`, but the mutation is a part-level operation, so
-- they ARE attributed to the resulting part in `system.part_log`. The same threshold reasoning as
-- above applies: the rebuilt projection is far smaller than the parent, so the threshold must sit
-- between them for the mutation's own sync decision to be the only thing that can sync it.
--
-- The comparison is against the SAME table's plain row-rewriting mutation rather than against a
-- projection-less table: an `ALTER ... UPDATE` that rewrites every row touches a different set of
-- files than a projection materialization, so only a before/after on one table is meaningful.
--
-- The bound below counts files, so nothing unrelated may add any. Statistics do: implicit
-- statistics put one more file in the mutated part, which is enough to satisfy the bound without
-- any projection file being synced. Both settings that produce them are randomized in CI
-- (`materialize_statistics_on_insert` to true in 95% of runs), so both must be pinned here.
SET materialize_statistics_on_insert = 0;

CREATE TABLE t_mat (id UInt64, key String, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS fsync_after_insert = 1, fsync_part_directory = 1,
         min_rows_to_fsync_after_merge = 1000, min_compressed_bytes_to_fsync_after_merge = 0,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         auto_statistics_types = '',
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_mat SELECT number, concat('k', toString(number % 7)), number FROM numbers(10000);
ALTER TABLE t_mat ADD PROJECTION pr (SELECT key, sum(v) GROUP BY key);
ALTER TABLE t_mat MATERIALIZE PROJECTION pr SETTINGS mutations_sync = 2;

-- Row-rewriting mutation: rebuilds the projection through the same temp-projection write path.
ALTER TABLE t_mat UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 2;

SYSTEM FLUSH LOGS part_log;

-- Every mutation that produced a part must have fsynced at least one projection file on top of
-- the parent's own files. The parent part is Wide with 3 columns, so its own syncs are bounded;
-- requiring strictly more than that bound proves projection files were synced too.
SELECT 'mutation syncs projection files',
    min(ProfileEvents['FileSync']) > 6
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_mat' AND event_type = 'MutatePart';

SELECT 'materialize rows', count() FROM t_mat;
SELECT sum(v) FROM t_mat GROUP BY key ORDER BY key SETTINGS force_optimize_projection = 1 FORMAT Null;
SELECT 'materialize check';
CHECK TABLE t_mat SETTINGS check_query_single_value_result = 1;

DROP TABLE t_mat;
