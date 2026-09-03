-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed planning requires the analyzer.

-- A shuffle exchange with a single bucket produces a one-task child fragment whose
-- receive step reads `bucket_id` from the task parameters. Combining it with a
-- multi-bucket sibling must keep that parameter intact.

DROP TABLE IF EXISTS t_one_bucket_l;
DROP TABLE IF EXISTS t_one_bucket_r;
CREATE TABLE t_one_bucket_l (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_one_bucket_r (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_one_bucket_l SELECT number, number * 10 FROM numbers(1000);
INSERT INTO t_one_bucket_r SELECT number, number * 100 FROM numbers(500);

-- make_distributed_plan rejects aggregation with a group-by row limit
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;

-- One-bucket shuffle join, multi-bucket readers
SELECT sum(l.v + r.w) FROM t_one_bucket_l AS l JOIN t_one_bucket_r AS r ON l.k = r.k
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0,
         distributed_plan_default_shuffle_join_bucket_count = 1, distributed_plan_default_reader_bucket_count = 3;

-- Same shape with the right side broadcast (replicated task merged with a partitioned sibling)
SELECT sum(l.v + r.w) FROM t_one_bucket_l AS l JOIN t_one_bucket_r AS r ON l.k = r.k
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0,
         distributed_plan_default_shuffle_join_bucket_count = 1, distributed_plan_default_reader_bucket_count = 3,
         distributed_plan_max_rows_to_broadcast = 1000000;

-- One-bucket shuffle join unioned with a plain multi-bucket read
SELECT sum(x) FROM (
  SELECT l.v AS x FROM t_one_bucket_l AS l JOIN t_one_bucket_r AS r ON l.k = r.k
  UNION ALL
  SELECT v AS x FROM t_one_bucket_l
)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0,
         distributed_plan_default_shuffle_join_bucket_count = 1, distributed_plan_default_reader_bucket_count = 3;

-- Same join under the cost-based optimizer
SELECT sum(l.v + r.w) FROM t_one_bucket_l AS l JOIN t_one_bucket_r AS r ON l.k = r.k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0,
         distributed_plan_default_shuffle_join_bucket_count = 1, distributed_plan_default_reader_bucket_count = 3;

DROP TABLE t_one_bucket_l;
DROP TABLE t_one_bucket_r;
