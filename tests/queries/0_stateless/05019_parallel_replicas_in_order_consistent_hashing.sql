-- Tags: no-random-merge-tree-settings

-- The in-order coordinator distributes mark segments between replicas by the same consistent hash
-- the default one uses, instead of handing the front of every part to whoever asked first. A replica
-- reads a part as a single sorted stream, so the placement must never move a replica backwards
-- inside a part - these checks fail immediately if it does, because the merged stream stops being
-- ordered (or loses/duplicates rows).

DROP TABLE IF EXISTS t_in_order_hashing;

CREATE TABLE t_in_order_hashing (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128;

-- Several parts, each big enough to be cut into many segments.
INSERT INTO t_in_order_hashing SELECT number, number FROM numbers_mt(300000);
INSERT INTO t_in_order_hashing SELECT number + 300000, number FROM numbers_mt(300000);
INSERT INTO t_in_order_hashing SELECT number + 600000, number FROM numbers_mt(400000);

SET enable_analyzer = 1;
SET optimize_read_in_order = 1;
SET max_threads = 4;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;

-- `a` is exactly the row number in the fully ordered stream, so comparing every row against its
-- position in the stream is an exact check that the merged result is complete and ordered.
SELECT 'ascending', count() = 1000000, countIf(a != rn) = 0
FROM (SELECT a, rowNumberInAllBlocks() AS rn FROM (SELECT a FROM t_in_order_hashing ORDER BY a));

SELECT 'descending', count() = 1000000, countIf(a != 999999 - rn) = 0
FROM (SELECT a, rowNumberInAllBlocks() AS rn FROM (SELECT a FROM t_in_order_hashing ORDER BY a DESC));

-- Reading only a prefix: the whole read is a couple of segments, so this is the case where a
-- replica is told there is nothing in a part for it right after its first request.
SELECT 'ascending limit', groupArray(a) FROM (SELECT a FROM t_in_order_hashing ORDER BY a LIMIT 5);
SELECT 'descending limit', groupArray(a) FROM (SELECT a FROM t_in_order_hashing ORDER BY a DESC LIMIT 5);

-- A filter leaves holes in the ranges the coordinator distributes, so segments no longer line up
-- with the part boundaries.
SELECT 'filtered', count() = 100000, countIf(a != 500000 + rn) = 0
FROM (SELECT a, rowNumberInAllBlocks() AS rn FROM (SELECT a FROM t_in_order_hashing WHERE a >= 500000 AND a < 600000 ORDER BY a));

-- More replicas than there is work for some of the parts.
SET max_parallel_replicas = 2;
SELECT 'two replicas', count() = 1000000, countIf(a != rn) = 0
FROM (SELECT a, rowNumberInAllBlocks() AS rn FROM (SELECT a FROM t_in_order_hashing ORDER BY a));

SET max_parallel_replicas = 3;
SET parallel_replicas_local_plan = 0;
SELECT 'no local plan', count() = 1000000, countIf(a != rn) = 0
FROM (SELECT a, rowNumberInAllBlocks() AS rn FROM (SELECT a FROM t_in_order_hashing ORDER BY a));

-- The smallest segmentation the coordinator allows: many more segments than replicas.
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_mark_segment_size = 128;
SELECT 'small segments', count() = 1000000, countIf(a != rn) = 0
FROM (SELECT a, rowNumberInAllBlocks() AS rn FROM (SELECT a FROM t_in_order_hashing ORDER BY a));

SELECT 'small segments descending', count() = 1000000, countIf(a != 999999 - rn) = 0
FROM (SELECT a, rowNumberInAllBlocks() AS rn FROM (SELECT a FROM t_in_order_hashing ORDER BY a DESC));

DROP TABLE t_in_order_hashing;
