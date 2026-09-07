-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- Tracked in https://github.com/ClickHouse/ClickHouse/issues/118534

-- `max_rows_in_join` is a limit on the whole right side of the join. A distributed shuffle join splits
-- the right side across `distributed_plan_default_shuffle_join_bucket_count` buckets, and every bucket's
-- `HashJoin` checks the limit against its own slice only, so a right side that exceeds the limit is
-- accepted. The query must throw exactly like it does with `make_distributed_plan = 0` (right side:
-- 1000 rows, limit: 900).

DROP TABLE IF EXISTS t_dp_join_l;
DROP TABLE IF EXISTS t_dp_join_r;
CREATE TABLE t_dp_join_l (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
INSERT INTO t_dp_join_l SELECT number % 20, number FROM numbers(10000);
CREATE TABLE t_dp_join_r (w UInt64) ENGINE = MergeTree ORDER BY w;
INSERT INTO t_dp_join_r SELECT number * 10 FROM numbers(1000);

SET enable_analyzer = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0;
-- Bucket every read and shuffle the join regardless of the table sizes.
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET distributed_plan_default_shuffle_join_bucket_count = 8;

SELECT 'max_rows_in_join';
SELECT count(), sum(r.w) FROM t_dp_join_l AS t JOIN t_dp_join_r AS r ON t.v = r.w
SETTINGS max_rows_in_join = 900, join_overflow_mode = 'throw', join_algorithm = 'hash', query_plan_join_swap_table = 'false'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

DROP TABLE t_dp_join_l;
DROP TABLE t_dp_join_r;
