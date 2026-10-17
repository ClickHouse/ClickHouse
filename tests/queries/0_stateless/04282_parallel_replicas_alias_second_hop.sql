-- Tags: distributed

-- INTENTIONAL: alias_base_0 ALIAS base and alias_base_1 ALIAS base reference the SAME
-- underlying expression. This shape detects column-COLLAPSE (two distinct logical
-- columns becoming one in transport because their expressions are identical and the
-- planner CSE deduplicates them), NOT column-SWAP. Column-swap regressions are covered
-- by 04280_distributed_alias_column_order. Do not "fix" by giving the two aliases
-- distinct values -- that would lose collapse coverage.

-- Regression coverage for materialized __aliasMarker metadata across
-- remote -> Distributed -> parallel replicas fanout.

DROP TABLE IF EXISTS test_alias_pr_second_hop_dist;
DROP TABLE IF EXISTS test_alias_pr_second_hop_local;

CREATE TABLE test_alias_pr_second_hop_local
(
    dt DateTime64(3),
    base String,
    alias_base_0 String ALIAS base,
    alias_base_1 String ALIAS base
)
ENGINE = MergeTree()
ORDER BY dt;

INSERT INTO test_alias_pr_second_hop_local VALUES
    ('1999-03-29T01:15:33', 'x'),
    ('1999-03-29T01:15:34', 'y');

CREATE TABLE test_alias_pr_second_hop_dist AS test_alias_pr_second_hop_local
ENGINE = Distributed(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_alias_pr_second_hop_local);

SELECT 'single_replica_second_hop';
SELECT dt, alias_base_0, alias_base_1
FROM remote('127.0.0.2', currentDatabase(), test_alias_pr_second_hop_dist)
ORDER BY dt
LIMIT 1
SETTINGS
    enable_analyzer = 1,
    enable_alias_marker = 1,
    enable_parallel_replicas = 1,
    max_parallel_replicas = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT 'parallel_replicas_second_hop';
SELECT dt, alias_base_0, alias_base_1
FROM remote('127.0.0.2', currentDatabase(), test_alias_pr_second_hop_dist)
ORDER BY dt
LIMIT 1
SETTINGS
    enable_analyzer = 1,
    enable_alias_marker = 1,
    enable_parallel_replicas = 1,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE test_alias_pr_second_hop_dist;
DROP TABLE test_alias_pr_second_hop_local;
