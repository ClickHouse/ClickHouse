-- LIMIT still shrinks the block size with arrayJoin, only the source-side limit stays off (#82279)
DROP TABLE IF EXISTS t_aj_limit;
CREATE TABLE t_aj_limit (k UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
-- the 5000 bound below needs an un-shrunk read above it, and marks = rows / index_granularity
INSERT INTO t_aj_limit SELECT number, [number] FROM numbers(20000);

-- the initiator's read_rows sums every replica's read, so the bound below needs a single reader
SELECT arrayJoin(a) FROM t_aj_limit LIMIT 1 FORMAT Null SETTINGS max_threads = 8, enable_parallel_replicas = 0, log_comment = '05182_limit';

SYSTEM FLUSH LOGS query_log;
SELECT argMax(read_rows, event_time_microseconds) < 5000 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05182_limit';

-- the block does not shrink below a few hundred rows, so a long run of empty arrays is not streamed one row at a time
SELECT DISTINCT bs FROM (SELECT arrayJoin(a), blockSize() AS bs FROM t_aj_limit LIMIT 3 SETTINGS max_threads = 1, enable_parallel_replicas = 0);

-- the prefetched pool sizes its reads by marks, not by the block size, so a small LIMIT keeps it off
-- it is only a candidate for a single-node read that is multi-stream or all-remote, and the local
-- read settings gate an all-local read while the remote ones gate an all-remote read
SELECT countIf(explain LIKE '%PrefetchedReadPool%') FROM (EXPLAIN PIPELINE SELECT arrayJoin(a) FROM t_aj_limit LIMIT 1 SETTINGS allow_prefetched_read_pool_for_local_filesystem = 1, local_filesystem_read_method = 'pread_threadpool', allow_prefetched_read_pool_for_remote_filesystem = 1, remote_filesystem_read_method = 'threadpool', max_threads = 8, max_threads_min_free_memory_per_thread = 0, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1, enable_parallel_replicas = 0);

DROP TABLE t_aj_limit;

-- a long empty-array prefix: the LIMIT is tiny but the source has to get past the prefix
DROP TABLE IF EXISTS t_aj_sparse;
CREATE TABLE t_aj_sparse (k UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO t_aj_sparse SELECT number, if(number < 9000, [], [number]) FROM numbers(10000);

-- one local stream reads in key order; with parallel replicas the order and the initiator's read_rows are not deterministic
SELECT arrayJoin(a) FROM t_aj_sparse LIMIT 3 SETTINGS max_threads = 1, enable_parallel_replicas = 0, log_comment = '05182_sparse';

SYSTEM FLUSH LOGS query_log;
SELECT argMax(read_rows, event_time_microseconds) >= 9000 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05182_sparse';

DROP TABLE t_aj_sparse;
