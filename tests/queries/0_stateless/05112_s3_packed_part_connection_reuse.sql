-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
-- no-fasttest: needs an S3 disk.

-- Files inside `data.packed` of a part with the Packed storage are read through a view over the archive.
-- The view must bound the request to the file's slice, otherwise every file read is an open-ended range request
-- that transfers the rest of the archive and the HTTP connection cannot be returned to the pool.

DROP TABLE IF EXISTS t_packed_s3;

CREATE TABLE t_packed_s3 (key UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64, c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64, c16 UInt64, c17 UInt64, c18 UInt64, c19 UInt64, c20 UInt64)
ENGINE = MergeTree ORDER BY key
SETTINGS storage_policy = 's3_no_cache', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 1000000000000, ratio_of_defaults_for_sparse_serialization = 1;

SYSTEM STOP MERGES t_packed_s3;

INSERT INTO t_packed_s3 SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(1000);

SELECT part_type, part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't_packed_s3' AND active;

-- One thread with synchronous reads: all streams can share one connection if it is released after each of them is read.
SELECT * FROM t_packed_s3 FORMAT Null
SETTINGS max_threads = 1, remote_filesystem_read_method = 'read', load_marks_asynchronously = 0, use_page_cache_for_disks_without_file_cache = 0, enable_parallel_replicas = 0, log_comment = '05112_s3_packed_part_connection_reuse';

SYSTEM FLUSH LOGS query_log;

-- Only the amount of reuse is checked: how many connections the query has to create depends on how many of them
-- are already sitting in the shared pool, which other queries running at the same time change.
SELECT
    ProfileEvents['S3GetObject'] >= 20 AS read_all_streams,
    ProfileEvents['DiskConnectionsReused'] >= 18 AS connections_reused
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05112_s3_packed_part_connection_reuse' AND type = 'QueryFinish' AND query_kind = 'Select';

DROP TABLE t_packed_s3;
