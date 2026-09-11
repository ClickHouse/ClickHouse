-- Tags: no-old-analyzer, no-parallel-replicas
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- no-parallel-replicas: the assertion is about the analysis the coordinator does for a distributed plan.

-- A distributed read used to analyze the index twice on the coordinator: `tryMakeDistributedRead`
-- analyzed once to compare the selected rows against `distributed_plan_max_rows_to_broadcast`, and
-- `setupDistributedReadBuckets` analyzed again to cut the read into mark buckets. The second analysis
-- re-ran over the ranges the first one had already narrowed, so it returned the same answer while
-- reading the indexes again - for a part on object storage, one blocking round trip per surviving mark
-- range. The logs showed it as two `Filtering marks by primary and secondary keys` passes, the second
-- one over the mark count the first one had selected.

DROP TABLE IF EXISTS t_distributed_plan_index_analysis;

CREATE TABLE t_distributed_plan_index_analysis (a UInt64, s String)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1024, index_granularity_bytes = 0;

INSERT INTO t_distributed_plan_index_analysis SELECT number, toString(number) FROM numbers(200000);

-- Distributed aggregation rejects a nonzero limit (randomized setting).
SET max_rows_to_group_by = 0;
-- A cached verdict would prune granules before analysis and hide how many marks it processes.
SET use_query_condition_cache = 0;

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 4;

-- The filter must leave something behind, otherwise a second analysis would have nothing to process.
SELECT count() FROM t_distributed_plan_index_analysis WHERE a < 100000
SETTINGS log_comment = '05175_distributed_plan_index_analysis_once' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- One pass over the index visits every mark of the table at most once. Analyzing twice processes the
-- marks selected by the first pass on top of that, which exceeds the table's mark count.
WITH (
    SELECT sum(marks)
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_distributed_plan_index_analysis' AND active
) AS total_marks
SELECT throwIf(ProfileEvents['FilteringMarksWithPrimaryKeyProcessedMarks'] > total_marks)
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '05175_distributed_plan_index_analysis_once'
  AND query LIKE '%count()%'
  AND type = 'QueryFinish'
FORMAT Null;

DROP TABLE t_distributed_plan_index_analysis;
