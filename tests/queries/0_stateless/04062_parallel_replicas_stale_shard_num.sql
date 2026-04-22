-- Regression test: querying a local MergeTree table with `cluster_for_parallel_replicas`
-- pointing to a multi-shard cluster must raise UNEXPECTED_CLUSTER (user-facing error)
-- instead of LOGICAL_ERROR (which could crash the server in debug builds).
--
-- Before fix: LOGICAL_ERROR
-- After fix: UNEXPECTED_CLUSTER
--
-- Note: The main stale `_shard_num` scenario (settings constraints causing mismatch between
-- initiator and remote) requires multi-server configuration and is covered by integration
-- test `test_parallel_replicas_stale_shard_num`. This stateless test verifies the
-- multi-shard error handling behavior that can be reproduced in single-server environment.

SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer
SET serialize_query_plan = 0;
SET allow_experimental_parallel_reading_from_replicas = 2;
SET max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (n UInt64) ENGINE = MergeTree() ORDER BY n;
INSERT INTO t SELECT * FROM numbers(10);

-- Query local MergeTree table with multi-shard cluster_for_parallel_replicas.
-- Use WHERE NOT ignore(*) to prevent trivial count optimization and ensure PR code path runs.
SET cluster_for_parallel_replicas = 'test_cluster_two_shards_localhost';
SELECT count() FROM t WHERE NOT ignore(*); -- { serverError UNEXPECTED_CLUSTER }

DROP TABLE t;
