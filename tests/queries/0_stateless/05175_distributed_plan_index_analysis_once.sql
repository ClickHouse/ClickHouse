-- Tags: no-old-analyzer, no-parallel-replicas
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- no-parallel-replicas: the assertion is about the analysis the coordinator does for a distributed plan.

-- Test to snure that distributed query planning does not repeat index analysis twice or more

DROP TABLE IF EXISTS t_distributed_plan_index_analysis;

CREATE TABLE t_distributed_plan_index_analysis (a UInt64, s String)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1024;

INSERT INTO t_distributed_plan_index_analysis SELECT number, toString(number) FROM numbers(200000);
-- One part, so a background merge cannot change the mark count the assertion compares against.
OPTIMIZE TABLE t_distributed_plan_index_analysis FINAL;

-- The filter must leave something behind, otherwise a second analysis would have nothing to process.
SELECT count() FROM t_distributed_plan_index_analysis WHERE a < 100000
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 4,
    max_rows_to_group_by = 0, use_query_condition_cache = 0,
    log_comment = '05175_distributed_plan_index_analysis_once'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- One pass over the index visits every mark of the table at most once. Analyzing twice processes the
-- marks selected by the first pass on top of that, which exceeds the table's mark count.
WITH (
    SELECT sum(marks)
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_distributed_plan_index_analysis' AND active
) AS total_marks
SELECT
    throwIf(count() = 0, 'the measured query is missing from query_log'),
    throwIf(max(ProfileEvents['FilteringMarksWithPrimaryKeyProcessedMarks']) > total_marks,
            'index analysis processed more marks than the table has, so it ran more than once')
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '05175_distributed_plan_index_analysis_once'
  AND query LIKE '%count()%'
  AND type = 'QueryFinish'
FORMAT Null;

DROP TABLE t_distributed_plan_index_analysis;
