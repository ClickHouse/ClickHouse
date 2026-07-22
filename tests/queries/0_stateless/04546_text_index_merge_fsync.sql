-- Tags: no-object-storage
-- no-object-storage: object storage does not fsync files.

-- Regression test for the merge text-index fsync gap (issue #111269).
-- With `min_rows_to_fsync_after_merge = 1` a merge fsyncs every file of the produced part.
-- Before the fix the text-index files (`skp_idx_<name>.*`) were the only ones left unsynced,
-- so a power loss after the part directory was fsynced left a committed but broken part.
-- The `text` index has three streams (`.idx`, `.dct`, `.pst`), each with a data and a marks
-- file, so a fully synced merge fsyncs exactly six files more than the same merge without the
-- index. We assert that exact delta, so a partial regression (only some streams synced) is
-- also caught. The fsync count is attributed to the `OPTIMIZE` query via its thread group, so
-- it is read from `system.query_log`. `max_bytes_to_merge_at_max_space_in_pool = 1` disables
-- background merges (so `OPTIMIZE` is the only merger and its fsyncs are attributed to it),
-- while `OPTIMIZE FINAL` ignores that limit.

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
