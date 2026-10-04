-- Tags: no-object-storage
-- no-object-storage: object storage does not fsync files.
-- Random settings limits: index_granularity=(100, None); index_granularity_bytes=(100000, None); merge_max_block_size=(100, None)

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

-- A merged projection is written straight into the final `<projection>.proj` of the result part,
-- so unlike the mutation path there is no rename to make the directory's own entries durable and
-- the merge has to fsync it. The delta against the identical plain merge is one per merged
-- projection.
SELECT 'merge projection adds directory syncs',
    (SELECT ProfileEvents['DirectorySync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Optimize'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_proj%' AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1)
    -
    (SELECT ProfileEvents['DirectorySync'] FROM system.query_log
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

-- The same directory-sync guarantee on `Packed` storage, where the whole projection is a single
-- `data.packed` archive written only when the part's transaction is precommitted. The directory
-- fsync has to happen after that, so cover both storage types.
CREATE TABLE t_packed (id UInt64, key String, v UInt64,
                       PROJECTION pr (SELECT key, sum(v) GROUP BY key))
ENGINE = MergeTree ORDER BY id
SETTINGS fsync_after_insert = 1, fsync_part_directory = 1,
         min_rows_to_fsync_after_merge = 1000, min_compressed_bytes_to_fsync_after_merge = 0,
         min_bytes_for_full_part_storage = 1000000000,
         max_bytes_to_merge_at_max_space_in_pool = 1;

CREATE TABLE t_packed_plain (id UInt64, key String, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS fsync_after_insert = 1, fsync_part_directory = 1,
         min_rows_to_fsync_after_merge = 1000, min_compressed_bytes_to_fsync_after_merge = 0,
         min_bytes_for_full_part_storage = 1000000000,
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_packed SELECT number, concat('k', toString(number % 7)), number FROM numbers(5000);
INSERT INTO t_packed_plain SELECT number, concat('k', toString(number % 7)), number FROM numbers(5000);

SYSTEM FLUSH LOGS query_log;

SELECT 'packed insert projection adds directory syncs',
    (SELECT max(ProfileEvents['DirectorySync']) FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Insert'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_packed %' AND type = 'QueryFinish')
    >
    (SELECT max(ProfileEvents['DirectorySync']) FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Insert'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_packed\_plain%' AND type = 'QueryFinish');

-- The directory assertion above cannot observe the archive's own fsync: on `Packed` storage a
-- projection file sync only sets `FinalizePlan::need_sync`, and `data.packed` is synced later just
-- when that flag is set. Dropping the projection's `sync` argument while keeping the directory
-- guard would therefore still pass. A `Packed` part has exactly one file to sync, so the delta is
-- exactly one.
SELECT 'packed insert projection adds exactly one file sync',
    (SELECT max(ProfileEvents['FileSync']) FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Insert'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_packed %' AND type = 'QueryFinish')
    -
    (SELECT max(ProfileEvents['FileSync']) FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Insert'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_packed\_plain%' AND type = 'QueryFinish');

SYSTEM FLUSH LOGS part_log;

-- Guards the assertions above: on `Full` storage the projection files are individually synced, so
-- confirm the fixture really produced a `Packed` part.
SELECT 'packed storage type', any(part_storage_type) FROM system.part_log
WHERE database = currentDatabase() AND table = 't_packed' AND event_type = 'NewPart';

SELECT 'packed check';
CHECK TABLE t_packed SETTINGS check_query_single_value_result = 1;

-- The merged projection's directory has to be fsynced on `Packed` storage too, and the ordering is
-- storage-sensitive there: `data.packed` is only finalized when the projection's transaction is
-- committed, so a guard taken before that would sync a directory not yet holding the archive.
INSERT INTO t_packed SELECT number + 5000, concat('k', toString(number % 7)), number FROM numbers(5000);
INSERT INTO t_packed_plain SELECT number + 5000, concat('k', toString(number % 7)), number FROM numbers(5000);

OPTIMIZE TABLE t_packed FINAL SETTINGS optimize_throw_if_noop = 1, alter_sync = 2;
OPTIMIZE TABLE t_packed_plain FINAL SETTINGS optimize_throw_if_noop = 1, alter_sync = 2;

SYSTEM FLUSH LOGS query_log;

SELECT 'packed merge projection adds directory syncs',
    (SELECT ProfileEvents['DirectorySync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Optimize'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_packed %' AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1)
    -
    (SELECT ProfileEvents['DirectorySync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Optimize'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_packed\_plain%' AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1);

-- The directory guard above is independent of the archive's own sync, exactly as on the insert
-- path: `data.packed` is written when the projection's transaction is committed and synced only if
-- the sub-merge inherited the parent's sync decision. Dropping that propagation while keeping the
-- guard would leave the assertion above green. A `Packed` projection is one archive, so the delta
-- is exactly one.
SELECT 'packed merge projection adds exactly one file sync',
    (SELECT ProfileEvents['FileSync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Optimize'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_packed %' AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1)
    -
    (SELECT ProfileEvents['FileSync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Optimize'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_packed\_plain%' AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1);

SYSTEM FLUSH LOGS part_log;

-- Guards the assertion above: it only covers the `Packed` ordering if the merge really produced a
-- `Packed` part.
SELECT 'packed merge storage type', argMax(part_storage_type, event_time_microseconds) FROM system.part_log
WHERE database = currentDatabase() AND table = 't_packed' AND event_type = 'MergeParts';

SELECT 'packed merge check';
CHECK TABLE t_packed SETTINGS check_query_single_value_result = 1;

DROP TABLE t_packed;
DROP TABLE t_packed_plain;

-- `MATERIALIZE PROJECTION` (mutation) path. Its fsyncs run on the IO thread pool and are not
-- attributed to the `ALTER` in `system.query_log`, but the mutation is a part-level operation, so
-- they ARE attributed to the resulting part in `system.part_log`. The same threshold reasoning as
-- above applies: the rebuilt projection is far smaller than the parent, so the threshold must sit
-- between them for the mutation's own sync decision to be the only thing that can sync it.
--
-- The reading is a DELTA against an identical projection-less table running the same mutation,
-- never an absolute file count: the number of files a part contains depends on the serialization
-- layout, and CI randomizes that (`serialization_info_version = 'basic'` drops `serialization.json`,
-- `string_serialization_version = 'single_stream'` drops the `key.size.*` pair), so any absolute
-- bound reads differently per run. The two tables share every setting, so a layout change moves
-- both readings and cancels out.
CREATE TABLE t_mat (id UInt64, key String, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS fsync_after_insert = 1, fsync_part_directory = 1,
         min_rows_to_fsync_after_merge = 1000, min_compressed_bytes_to_fsync_after_merge = 0,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1;

CREATE TABLE t_mat_plain (id UInt64, key String, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS fsync_after_insert = 1, fsync_part_directory = 1,
         min_rows_to_fsync_after_merge = 1000, min_compressed_bytes_to_fsync_after_merge = 0,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_mat SELECT number, concat('k', toString(number % 7)), number FROM numbers(10000);
INSERT INTO t_mat_plain SELECT number, concat('k', toString(number % 7)), number FROM numbers(10000);
ALTER TABLE t_mat ADD PROJECTION pr (SELECT key, sum(v) GROUP BY key);
ALTER TABLE t_mat MATERIALIZE PROJECTION pr SETTINGS mutations_sync = 2;

-- The compared mutations must sit at the same position in their part's mutation chain: a mutation's
-- sync count depends on that position, not only on the files it writes (measured on a table with no
-- projection at all, the same `UPDATE` syncs 7 files as the first mutation and 9 as the second). This
-- no-op mutation matches the materialization above so the compared pair is the second on both sides.
ALTER TABLE t_mat_plain UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2;

-- Row-rewriting mutation: rebuilds the projection through the same temp-projection write path.
ALTER TABLE t_mat UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_mat_plain UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 2;

SYSTEM FLUSH LOGS part_log;

-- The projection rebuild the mutation performs must fsync the projection on top of everything the
-- identical projection-less mutation already syncs.
--
-- The delta is one, and stays one however many columns the projection has, because of what this
-- counter can see and not because of how much is synced: `part_log` takes its `ProfileEvents` from a
-- scope that counts the mutation thread's own events only, and `parallelSyncFiles` hands every batch
-- of two or more files to the shared IO pool. The projection's columns and its metadata files are
-- synced in such batches and are invisible here, while its primary index is synced on its own,
-- inline, and is the one fsync that reaches this counter. Asserting the exact value means a missing
-- sync and a stray extra one both redden.
SELECT 'mutation syncs projection files',
    (SELECT argMax(ProfileEvents['FileSync'], event_time_microseconds) FROM system.part_log
     WHERE database = currentDatabase() AND table = 't_mat' AND event_type = 'MutatePart')
    -
    (SELECT argMax(ProfileEvents['FileSync'], event_time_microseconds) FROM system.part_log
     WHERE database = currentDatabase() AND table = 't_mat_plain' AND event_type = 'MutatePart');

SELECT 'materialize rows', count() FROM t_mat;
SELECT sum(v) FROM t_mat GROUP BY key ORDER BY key SETTINGS force_optimize_projection = 1 FORMAT Null;
SELECT 'materialize check';
CHECK TABLE t_mat SETTINGS check_query_single_value_result = 1;

DROP TABLE t_mat;
DROP TABLE t_mat_plain;
