-- Tags: no-parallel, shard
-- - no-parallel: uses a fail point (global server state)
-- - shard: needs the test cluster to build a Distributed table

DROP TABLE IF EXISTS t_rqe_cancel_local SYNC;
DROP TABLE IF EXISTS t_rqe_cancel_dist SYNC;

CREATE TABLE t_rqe_cancel_local (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_rqe_cancel_local SELECT number FROM numbers(100);

CREATE TABLE t_rqe_cancel_dist (a UInt64)
ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_rqe_cancel_local);

SYSTEM ENABLE FAILPOINT remote_query_executor_cancel_before_send;

-- prefer_localhost_replica = 0 forces a RemoteQueryExecutor for every shard so
-- the fail point can flip was_cancelled before the query is sent. Without the
-- fix the cancelled shard's null `connections` is dereferenced and the server
-- crashes. The crash is immediate, so the short max_execution_time bound (the
-- fail point makes this synthetic query wait for a shard that never sends) is
-- only a safety net and does not affect whether the crash is caught.
SELECT * FROM t_rqe_cancel_dist
SETTINGS skip_unavailable_shards = 1, prefer_localhost_replica = 0,
         async_socket_for_remote = 0, async_query_sending_for_remote = 0,
         max_threads = 1, max_execution_time = 3, timeout_overflow_mode = 'break'
FORMAT Null;

SYSTEM DISABLE FAILPOINT remote_query_executor_cancel_before_send;

-- The server must still be alive (a crash would have killed it).
SELECT 'ok';

DROP TABLE t_rqe_cancel_local SYNC;
DROP TABLE t_rqe_cancel_dist SYNC;
