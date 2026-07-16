-- Tags: no-parallel-replicas
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/50593
-- Custom key parallel replicas must still merge per-replica partial results on the initiator
-- when the custom key is not a function of the GROUP BY keys (e.g. plain count()).
-- Previously distributed_group_by_no_merge=2 was set unconditionally, so count() returned
-- one partial row per replica instead of the merged total.

DROP TABLE IF EXISTS t_04545 SYNC;

CREATE TABLE t_04545 (number Int64, y UInt32) ENGINE = MergeTree ORDER BY number;
INSERT INTO t_04545 SELECT number, number % 3 FROM numbers(100000);

SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_mode = 'custom_key_sampling';
SET automatic_parallel_replicas_mode = 0;

-- Plain count(): custom key is NOT a GROUP BY key -> must be merged -> single row.
SELECT 'count analyzer=1';
SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
SETTINGS parallel_replicas_custom_key = 'sipHash64(number)', enable_analyzer = 1;

SELECT 'count analyzer=0';
SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
SETTINGS parallel_replicas_custom_key = 'sipHash64(number)', enable_analyzer = 0;

-- count() wrapped in a subquery (exact shape from the issue) -> single row.
SELECT 'count subquery';
SELECT * FROM (SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545))
SETTINGS parallel_replicas_custom_key = 'sipHash64(number)';

-- sum() forces a materialization (different aggregate path than count()); custom key is NOT a
-- GROUP BY key -> per-replica partials must be merged on the initiator -> single total row.
SELECT 'sum analyzer=1';
SELECT sum(number) FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
SETTINGS parallel_replicas_custom_key = 'sipHash64(number)', enable_analyzer = 1;

SELECT 'sum analyzer=0';
SELECT sum(number) FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
SETTINGS parallel_replicas_custom_key = 'sipHash64(number)', enable_analyzer = 0;

-- GROUP BY on a key that does NOT cover the custom key -> must be merged.
SELECT 'group by not covering custom key';
SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'sipHash64(number)';

-- GROUP BY covers the custom key (custom key is a function of the GROUP BY key) -> correct with or
-- without merge; results must still be the merged totals.
SELECT 'group by covering custom key (y)';
SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'y', enable_analyzer = 1;

SELECT 'group by covering custom key cityHash64(y)';
SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'cityHash64(y)', enable_analyzer = 1;

-- Non-deterministic custom key (references only the GROUP BY column y, but rand() scatters rows of
-- the same group across replicas) -> must NOT skip the merge -> single merged row per group.
-- Assert the number of output rows is 3 (merged), not one partial row per replica.
SELECT 'non-deterministic custom key merged rows analyzer=1';
SELECT count() FROM (
    SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
    GROUP BY y SETTINGS parallel_replicas_custom_key = 'y + rand()', enable_analyzer = 1
) SETTINGS enable_analyzer = 1;

SELECT 'non-deterministic custom key merged rows analyzer=0';
SELECT count() FROM (
    SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
    GROUP BY y SETTINGS parallel_replicas_custom_key = 'y + rand()', enable_analyzer = 0
) SETTINGS enable_analyzer = 0;

-- Expression GROUP BY key with a custom key that is a deterministic function of that expression
-- (custom key equals the GROUP BY expression) -> safe to skip the merge; results must still be the
-- merged totals.
SELECT 'expression group by covering custom key mod(number,3) analyzer=1';
SELECT mod(number, 3) AS m, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY mod(number, 3) ORDER BY m
SETTINGS parallel_replicas_custom_key = 'mod(number, 3)', enable_analyzer = 1;

SELECT 'expression group by covering custom key mod(number,3) analyzer=0';
SELECT mod(number, 3) AS m, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY mod(number, 3) ORDER BY m
SETTINGS parallel_replicas_custom_key = 'mod(number, 3)', enable_analyzer = 0;

DROP TABLE t_04545 SYNC;
