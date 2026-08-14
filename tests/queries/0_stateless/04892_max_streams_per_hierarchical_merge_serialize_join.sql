-- Tags: no-old-analyzer

DROP TABLE IF EXISTS t_hierarchical_merge_join_left;
DROP TABLE IF EXISTS t_hierarchical_merge_join_right;

CREATE TABLE t_hierarchical_merge_join_left (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_hierarchical_merge_join_right (id UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_hierarchical_merge_join_left SELECT number FROM numbers(10);
INSERT INTO t_hierarchical_merge_join_right SELECT number FROM numbers(10);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_default_shuffle_join_bucket_count = 3;
SET distributed_plan_default_reader_bucket_count = 3;
SET distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0;
SET enable_join_runtime_filters = 0;
SET max_streams_per_hierarchical_merge = 1;

SELECT count()
FROM t_hierarchical_merge_join_left AS l
INNER JOIN t_hierarchical_merge_join_right AS r ON l.id = r.id
SETTINGS make_distributed_plan = 1, join_algorithm = 'hash';

SELECT count()
FROM t_hierarchical_merge_join_left AS l
INNER JOIN t_hierarchical_merge_join_right AS r ON l.id = r.id
SETTINGS make_distributed_plan = 1, join_algorithm = 'full_sorting_merge'; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_hierarchical_merge_join_left;
DROP TABLE t_hierarchical_merge_join_right;
