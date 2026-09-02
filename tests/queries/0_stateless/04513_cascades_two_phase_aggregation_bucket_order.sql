-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.

-- The memory-efficient distributed merge expects every input to deliver two-level buckets in
-- ascending order, so the two-phase split makes the partial step emit them that way.  Without
-- that a parallel flush would unite several bucket sequences into one exchange stream out of
-- order and the merge would emit some groups twice.  The query is repeated because such
-- duplication depends on chunk arrival order and would not appear on every run.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- Force the two-level aggregation and the memory-efficient merge, and keep several threads so the
-- partial flush runs in parallel.
SET group_by_two_level_threshold = 1;
SET distributed_aggregation_memory_efficient = 1;
SET max_threads = 4;

DROP TABLE IF EXISTS agg_bo;
CREATE TABLE agg_bo (k UInt64, s UInt64, g1 UInt16, g2 UInt16) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';

-- A large table-size hint makes Cascades pick the distributed two-phase plan (partial aggregation
-- per shard, gathered and merged) rather than reading and aggregating everything on one node.
SET param__internal_join_table_stat_hints = '{"agg_bo": {"cardinality": 600000000, "avg_row_bytes": 24, "distinct_keys": {"g1": 200, "g2": 150}}}';

INSERT INTO agg_bo SELECT number, number % 500, number % 200, intDiv(number, 40) % 150 FROM numbers(1000000);

-- Every output row must be a distinct group; a duplicated group means partial states were
-- not merged.  10 repetitions because a single run can pass by chance.
SELECT throwIf(count() != uniqExact((g1, g2)), 'duplicate groups in two-phase aggregation') FROM (SELECT g1, g2, uniqExact(s) AS u FROM agg_bo GROUP BY g1, g2);
SELECT throwIf(count() != uniqExact((g1, g2)), 'duplicate groups in two-phase aggregation') FROM (SELECT g1, g2, uniqExact(s) AS u FROM agg_bo GROUP BY g1, g2);
SELECT throwIf(count() != uniqExact((g1, g2)), 'duplicate groups in two-phase aggregation') FROM (SELECT g1, g2, uniqExact(s) AS u FROM agg_bo GROUP BY g1, g2);
SELECT throwIf(count() != uniqExact((g1, g2)), 'duplicate groups in two-phase aggregation') FROM (SELECT g1, g2, uniqExact(s) AS u FROM agg_bo GROUP BY g1, g2);
SELECT throwIf(count() != uniqExact((g1, g2)), 'duplicate groups in two-phase aggregation') FROM (SELECT g1, g2, uniqExact(s) AS u FROM agg_bo GROUP BY g1, g2);
SELECT throwIf(count() != uniqExact((g1, g2)), 'duplicate groups in two-phase aggregation') FROM (SELECT g1, g2, uniqExact(s) AS u FROM agg_bo GROUP BY g1, g2);
SELECT throwIf(count() != uniqExact((g1, g2)), 'duplicate groups in two-phase aggregation') FROM (SELECT g1, g2, uniqExact(s) AS u FROM agg_bo GROUP BY g1, g2);
SELECT throwIf(count() != uniqExact((g1, g2)), 'duplicate groups in two-phase aggregation') FROM (SELECT g1, g2, uniqExact(s) AS u FROM agg_bo GROUP BY g1, g2);
SELECT throwIf(count() != uniqExact((g1, g2)), 'duplicate groups in two-phase aggregation') FROM (SELECT g1, g2, uniqExact(s) AS u FROM agg_bo GROUP BY g1, g2);
SELECT throwIf(count() != uniqExact((g1, g2)), 'duplicate groups in two-phase aggregation') FROM (SELECT g1, g2, uniqExact(s) AS u FROM agg_bo GROUP BY g1, g2);

-- The result must also match the non-distributed baseline.
SELECT count(), uniqExact((g1, g2)), sum(u) FROM (SELECT g1, g2, uniqExact(s) AS u FROM agg_bo GROUP BY g1, g2);
SELECT count(), uniqExact((g1, g2)), sum(u) FROM (SELECT g1, g2, uniqExact(s) AS u FROM agg_bo GROUP BY g1, g2)
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE agg_bo;
