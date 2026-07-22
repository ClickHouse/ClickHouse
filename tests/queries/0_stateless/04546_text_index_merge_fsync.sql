-- Tags: no-object-storage
-- no-object-storage: object storage does not fsync files.

-- Regression test for the text-index merge fsync gap (issue #111269).
-- A `text` index has three streams (`.idx`, `.dct`, `.pst`), each with a data and a marks file,
-- so a fully synced merge fsyncs exactly six files more than the same merge without the index.
-- The fsync count is attributed to the `OPTIMIZE` query, so it is read from `system.query_log`.
-- `max_bytes_to_merge_at_max_space_in_pool = 1` disables background merges so `OPTIMIZE FINAL`
-- (which ignores that limit) is the only merger and its fsyncs are attributed to it.

SET materialize_skip_indexes_on_insert = 0;

DROP TABLE IF EXISTS t_txt;
DROP TABLE IF EXISTS t_plain;

CREATE TABLE t_txt (id UInt64, key String, INDEX it(key) TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_to_fsync_after_merge = 1, min_compressed_bytes_to_fsync_after_merge = 1,
         fsync_part_directory = 1, fsync_after_insert = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1;

CREATE TABLE t_plain (id UInt64, key String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_to_fsync_after_merge = 1, min_compressed_bytes_to_fsync_after_merge = 1,
         fsync_part_directory = 1, fsync_after_insert = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1;

-- Two parts each; the text index is materialized only when the parts are merged.
INSERT INTO t_txt SELECT number, concat('k', toString(number % 7)) FROM numbers(5000);
INSERT INTO t_txt SELECT number + 5000, concat('k', toString(number % 7)) FROM numbers(5000);
INSERT INTO t_plain SELECT number, concat('k', toString(number % 7)) FROM numbers(5000);
INSERT INTO t_plain SELECT number + 5000, concat('k', toString(number % 7)) FROM numbers(5000);

OPTIMIZE TABLE t_txt FINAL SETTINGS optimize_throw_if_noop = 1, alter_sync = 2;
OPTIMIZE TABLE t_plain FINAL SETTINGS optimize_throw_if_noop = 1, alter_sync = 2;

SYSTEM FLUSH LOGS query_log;

-- The text-index merge must fsync exactly the six text-index files (three streams, data + marks)
-- more than the identical plain merge.
SELECT 'extra text index file syncs on merge',
    (SELECT ProfileEvents['FileSync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Optimize'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_txt%' AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1)
    -
    (SELECT ProfileEvents['FileSync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND query_kind = 'Optimize'
       AND query NOT LIKE '%query_log%' AND query LIKE '%t\_plain%' AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1);

-- The materialized part must be readable and complete after the merge.
SELECT 'rows', count() FROM t_txt;
SELECT 'hasToken', count() FROM t_txt WHERE hasToken(key, 'k3');

DROP TABLE t_txt;
DROP TABLE t_plain;

-- The shared writer `MergeTextIndexesTask::finalize` has a second caller: a wide-part
-- `ALTER TABLE ... MATERIALIZE INDEX` (`MutateTask.cpp`). Its fsyncs run on the IO thread pool
-- and are attributed neither to the `ALTER` in `system.query_log` nor to the `MutatePart` row in
-- `system.part_log`, so the fsync delta is not observable here the way the merge delta is. This
-- block instead exercises the mutation entry point end to end and asserts the materialized index
-- files actually exist on disk and are used, not merely that a full-scan fallback returns the
-- right count (a text-index reader falls back to a full scan when a part has no index files, so a
-- count alone passes even if the index is absent or broken).

CREATE TABLE t_mat (id UInt64, key String, INDEX it(key) TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_to_fsync_after_merge = 1, min_compressed_bytes_to_fsync_after_merge = 1,
         fsync_part_directory = 1, fsync_after_insert = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1, index_granularity = 1024;

-- One wide part; the text index is built only by the MATERIALIZE INDEX mutation. The rare token
-- `zzrare` lives only in the first granule so a text-index lookup can skip the other granules.
INSERT INTO t_mat SELECT number, if(number < 50, 'zzrare', concat('k', toString(number % 7))) FROM numbers(10000);

SELECT 'materialized index bytes before', sum(secondary_indices_compressed_bytes) FROM system.parts
WHERE database = currentDatabase() AND table = 't_mat' AND active;

ALTER TABLE t_mat MATERIALIZE INDEX it SETTINGS mutations_sync = 2;

-- A wide part is produced, the index files exist on disk (non-zero size), the index actually
-- prunes granules (so the reader used it, not the full-scan fallback), and the part is readable.
SELECT 'materialized part_type', part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_mat' AND active;
SELECT 'materialized index bytes after > 0', sum(secondary_indices_compressed_bytes) > 0 FROM system.parts
WHERE database = currentDatabase() AND table = 't_mat' AND active;
SELECT 'materialized index prunes granules', countIf(explain LIKE '%Granules: 1/%') = 1
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_mat WHERE hasToken(key, 'zzrare')
      SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0);
SELECT 'materialized rows', count() FROM t_mat;

DROP TABLE t_mat;
