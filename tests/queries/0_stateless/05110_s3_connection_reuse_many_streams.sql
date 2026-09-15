-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
-- no-fasttest: needs an S3 disk.

-- Every stream of a Wide part is read up to the end of its data file. The read buffer must pass the exact right
-- bound to the object storage request even when it coincides with the end of the object, otherwise the HTTP connection
-- is never returned to the pool and every stream opens a new connection.

DROP TABLE IF EXISTS t_many_streams_s3;

CREATE TABLE t_many_streams_s3 (key UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64, c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64, c16 UInt64, c17 UInt64, c18 UInt64, c19 UInt64, c20 UInt64)
ENGINE = MergeTree ORDER BY key
SETTINGS storage_policy = 's3_no_cache', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

SYSTEM STOP MERGES t_many_streams_s3;

INSERT INTO t_many_streams_s3 SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(1000);

-- One thread with synchronous reads: all streams can share one connection if it is released after each of them is read to the end.
SELECT * FROM t_many_streams_s3 FORMAT Null
SETTINGS max_threads = 1, remote_filesystem_read_method = 'read', load_marks_asynchronously = 0, use_page_cache_for_disks_without_file_cache = 0, enable_parallel_replicas = 0, log_comment = '05110_s3_connection_reuse_many_streams';

SYSTEM FLUSH LOGS query_log;

-- The connection pool is shared with everything else running on the server, so the query can lose a
-- connection to a concurrent query and create another one. Assert that reuse dominates creation
-- instead of pinning the number of created connections: before the fix every stream held its own
-- connection to the end of the query, so there was nothing to reuse at all.
SELECT
    ProfileEvents['S3GetObject'] >= 20 AS read_all_streams,
    ProfileEvents['DiskConnectionsCreated'] < ProfileEvents['DiskConnectionsReused'] AS reuse_dominates,
    ProfileEvents['DiskConnectionsReused'] >= 18 AS connections_reused
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05110_s3_connection_reuse_many_streams' AND type = 'QueryFinish' AND query_kind = 'Select';

DROP TABLE t_many_streams_s3;
