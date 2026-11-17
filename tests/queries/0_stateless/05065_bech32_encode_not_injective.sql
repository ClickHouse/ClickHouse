-- Tags: no-fasttest, distributed
-- no-fasttest: bech32Encode is not built in the fast-test build.
-- https://github.com/ClickHouse/ClickHouse/issues/117201
-- `bech32Encode` returns an empty string on any encoding error - most easily, data too long for a
-- valid Bech32 string - so arbitrarily many distinct inputs share that result. It must not claim to
-- be injective, or the default-on `optimize_injective_functions_in_group_by` and
-- `optimize_injective_functions_in_limit_by` group and limit by the raw argument instead.

SELECT 'ground truth';
SELECT DISTINCT bech32Encode('bc', repeat('x', 60) || toString(number)) = '' FROM numbers(4);

SELECT 'group by';
SELECT count() FROM (SELECT 1 FROM numbers(4) GROUP BY bech32Encode('bc', repeat('x', 60) || toString(number)));
SELECT count() FROM (SELECT 1 FROM numbers(4) GROUP BY bech32Encode('bc', repeat('x', 60) || toString(number))) SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT 'limit by';
SELECT count() FROM (SELECT bech32Encode('bc', repeat('x', 60) || toString(number)) AS k FROM numbers(4) LIMIT 1 BY k);
SELECT count() FROM (SELECT bech32Encode('bc', repeat('x', 60) || toString(number)) AS k FROM numbers(4) LIMIT 1 BY k) SETTINGS optimize_injective_functions_in_limit_by = 0;

SELECT 'valid data still round-trips';
SELECT bech32Decode(bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 0)).2 = unhex('751e76e8199196d454941c45d1b3a323f1433bd6');

-- `EXPLAIN QUERY TREE` and the two planner paths tested below exist in the analyzer only.
SET enable_analyzer = 1;

SELECT 'order by truncation';
-- `optimize_truncate_order_by_after_group_by_keys` drops the ORDER BY elements after the ones that
-- cover every `GROUP BY` key, and an injective function of a key covers that key. Dropping the `s`
-- tiebreaker here would leave the order among the rows that encode to an empty string unspecified.
SELECT countSubstrings(arrayStringConcat(groupArray(explain), '\n'), 'SORT id')
FROM (EXPLAIN QUERY TREE
    SELECT s FROM (SELECT repeat('x', 60) || toString(number) AS s FROM numbers(4))
    GROUP BY s ORDER BY bech32Encode('bc', s), s);
SELECT countSubstrings(arrayStringConcat(groupArray(explain), '\n'), 'SORT id')
FROM (EXPLAIN QUERY TREE
    SELECT s FROM (SELECT repeat('x', 60) || toString(number) AS s FROM numbers(4))
    GROUP BY s ORDER BY bech32Encode('bc', s), s
    SETTINGS optimize_truncate_order_by_after_group_by_keys = 0);

SELECT 'independent aggregation per partition';
-- Aggregating every partition on its own is correct only when equal `GROUP BY` keys imply one
-- partition, which the planner concludes by stripping injective functions off the keys. All the
-- values below encode to an empty string while living in eight different partitions.
DROP TABLE IF EXISTS bech32_partitioned;
CREATE TABLE bech32_partitioned (p String, v UInt64) ENGINE = MergeTree ORDER BY tuple() PARTITION BY p;
INSERT INTO bech32_partitioned SELECT repeat('x', 60) || toString(number), number FROM numbers(8);
SET optimize_injective_functions_in_group_by = 0;
SELECT count() FROM (SELECT sum(v) FROM bech32_partitioned GROUP BY bech32Encode('bc', p))
SETTINGS allow_aggregate_partitions_independently = 1, force_aggregate_partitions_independently = 1;
SELECT count() FROM (SELECT sum(v) FROM bech32_partitioned GROUP BY bech32Encode('bc', p))
SETTINGS allow_aggregate_partitions_independently = 0;
DROP TABLE bech32_partitioned;

SELECT 'distributed sharding key';
-- `StorageDistributed::getOptimizedQueryProcessingStageAnalyzer` strips injective functions off the
-- `GROUP BY`, `DISTINCT` and `LIMIT BY` keys, and when what remains is the sharding key it lets every
-- shard finish the query on its own, skipping the merge on the initiator. Both shards of
-- `test_cluster_two_shards` read the same local table, so a query that is wrongly completed
-- shard-locally returns every group, distinct value or limited row twice.
DROP TABLE IF EXISTS bech32_local;
DROP TABLE IF EXISTS bech32_dist;
CREATE TABLE bech32_local (p String, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO bech32_local SELECT repeat('x', 60) || toString(number), number FROM numbers(8);
CREATE TABLE bech32_dist AS bech32_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), bech32_local, sipHash64(p));
SET optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1, optimize_injective_functions_in_limit_by = 0;
SELECT count() FROM (SELECT sum(v) FROM bech32_dist GROUP BY bech32Encode('bc', p));
SELECT count() FROM (SELECT DISTINCT bech32Encode('bc', p) FROM bech32_dist);
SELECT count() FROM (SELECT bech32Encode('bc', p) AS k FROM bech32_dist LIMIT 1 BY k);
-- The same three queries with the shard-local completion turned off.
SELECT count() FROM (SELECT sum(v) FROM bech32_dist GROUP BY bech32Encode('bc', p)) SETTINGS optimize_distributed_group_by_sharding_key = 0;
SELECT count() FROM (SELECT DISTINCT bech32Encode('bc', p) FROM bech32_dist) SETTINGS optimize_distributed_group_by_sharding_key = 0;
SELECT count() FROM (SELECT bech32Encode('bc', p) AS k FROM bech32_dist LIMIT 1 BY k) SETTINGS optimize_distributed_group_by_sharding_key = 0;
DROP TABLE bech32_dist;
DROP TABLE bech32_local;
