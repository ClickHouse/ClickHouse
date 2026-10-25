-- Tags: no-fasttest, no-old-analyzer
-- no-fasttest: needs the streaming exchange of the stateless worker configuration.
-- no-old-analyzer: distributed planning requires the analyzer.

-- The merge of partial aggregation results after a gather expects the buckets of every sender on
-- one input stream, in order. So the gather receive keeps one stream per source, however many
-- threads the receiving task has. Without that the merge sees a bucket twice and fails.

DROP TABLE IF EXISTS t_gather_merge;
-- The statistics of `g` make the planner aggregate partially on the readers and merge after a gather.
CREATE TABLE t_gather_merge (g UInt32, v UInt64) ENGINE = MergeTree ORDER BY tuple()
  SETTINGS auto_statistics_types = 'basic, uniq_v2';
-- The plan below needs the group count estimate, so the statistics must exist right after the insert.
SET materialize_statistics_on_insert = 1;
INSERT INTO t_gather_merge SELECT number % 1000, number FROM numbers(1000000);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0, use_statistics = 1;
SET distributed_plan_default_reader_bucket_count = 3, distributed_plan_default_shuffle_join_bucket_count = 3;
SET max_threads = 8;
SET explain_query_plan_default = 'legacy';
EXPLAIN SELECT g, count() FROM t_gather_merge GROUP BY g;
-- Every partial result is two-level, so the merge works bucket by bucket.
SELECT count(), sum(c) FROM (SELECT g, count() AS c FROM t_gather_merge GROUP BY g)
  SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1,
           enable_memory_bound_merging_of_aggregation_results = 1, distributed_aggregation_memory_efficient = 1;

DROP TABLE t_gather_merge;
