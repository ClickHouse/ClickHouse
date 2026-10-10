-- Tags: distributed

-- Smoke test for the `distributed_query_retries` and `distributed_query_retry_interval_ms` settings:
-- distributed queries must work the same way when the retries are enabled.
-- The retry itself (a replica stopped while a query is receiving data) requires killing a server
-- in the middle of a query and cannot be reproduced in a stateless test.

DROP TABLE IF EXISTS t1_shard;
DROP TABLE IF EXISTS t2_shard;
DROP TABLE IF EXISTS t1_distr;
DROP TABLE IF EXISTS t2_distr;

CREATE TABLE t1_shard (id Int32, value String) ENGINE = MergeTree PARTITION BY id ORDER BY id;
CREATE TABLE t2_shard (id Int32, value String) ENGINE = MergeTree PARTITION BY id ORDER BY id;

CREATE TABLE t1_distr AS t1_shard ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t1_shard, id);
CREATE TABLE t2_distr AS t2_shard ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t2_shard, id);

INSERT INTO t1_shard VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t2_shard VALUES (1, 'a'), (2, 'b'), (3, 'c');

SET distributed_product_mode = 'global';
SET distributed_query_retries = 3;
SET distributed_query_retry_interval_ms = 100;

-- Both "shards" of `test_cluster_two_shards_localhost` point to the same local table, so every row
-- exists on both shards. Any optimization that pushes `DISTINCT`/`GROUP BY` down to the shards
-- (they are randomized in CI) would skip the deduplication on the initiator and duplicate the result.
SET optimize_skip_unused_shards = 0;
SET optimize_distributed_group_by_sharding_key = 0;
SET distributed_group_by_no_merge = 0;
SET enable_parallel_replicas = 0;

-- Simulate data loss on a replica by detaching a partition: the query must succeed
-- (no retries are triggered — a missing partition is not a network error).
ALTER TABLE t1_shard DETACH PARTITION 1;

SELECT DISTINCT d0.id, d0.value
FROM t1_distr d0
WHERE d0.id IN
(
    SELECT d1.id
    FROM t1_distr AS d1
    INNER JOIN t2_distr AS d2 ON d1.id = d2.id
    WHERE d1.id > 0
    ORDER BY d1.id
)
ORDER BY d0.id;

-- Reattach the partition to restore the data.
ALTER TABLE t1_shard ATTACH PARTITION 1;

SELECT DISTINCT d0.id, d0.value
FROM t1_distr d0
JOIN
(
    SELECT d1.id, d1.value
    FROM t1_distr AS d1
    INNER JOIN t2_distr AS d2 ON d1.id = d2.id
    WHERE d1.id > 0
    ORDER BY d1.id
) s0 USING (id, value)
ORDER BY d0.id;

DROP TABLE t1_shard;
DROP TABLE t2_shard;
DROP TABLE t1_distr;
DROP TABLE t2_distr;
