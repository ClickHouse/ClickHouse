-- `parallel_replicas_custom_key` is an arbitrary expression, and it does not have to describe exactly one
-- column: `tuple()` describes no columns at all. Reading the type of the only column of such a key read
-- past the end of the types of the key and killed the server.
--
-- The table is a plain `MergeTree`, so the queries enable `parallel_replicas_for_non_replicated_merge_tree`.
-- Without it the old analyzer executes them without the parallel replicas, and the filter by the custom key,
-- which is built together with them, is not built at all.

DROP TABLE IF EXISTS d;
DROP TABLE IF EXISTS t;

CREATE TABLE t (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT number, number FROM numbers(1000);

CREATE TABLE d AS t ENGINE = Distributed('test_cluster_two_shard_three_replicas_localhost', currentDatabase(), t);

SELECT count() FROM d
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'tuple()'; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

SELECT count() FROM d
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = '(x, y)'; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

SELECT count(), sum(y) FROM d
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'x';

DROP TABLE d;
DROP TABLE t;
