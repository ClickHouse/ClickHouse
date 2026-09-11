-- Tags: no-old-analyzer, no-parallel-replicas
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- no-parallel-replicas: the assertion is about the analysis the coordinator does for a distributed plan.

-- Planning a distributed read used to analyze the index three times on the coordinator, because
-- `selectRangesToRead` re-analyzes on every call: once to compare the selected rows against
-- `distributed_plan_max_rows_to_broadcast`, once in `setupDistributedReadBuckets` to cut the read into
-- mark buckets, and once in `getShardsForDistributedRead`. Only the first pass did any work; the other
-- two re-ran over the ranges it had already narrowed and returned the same answer, while reading the
-- indexes again - for a part on object storage, one blocking round trip per surviving mark range. In
-- the logs it looked like three `Filtering marks by primary and secondary keys` passes, the second and
-- third over the mark count the first one had selected (196, then 98, then 98 granules here).

DROP TABLE IF EXISTS t_distributed_plan_index_analysis;

CREATE TABLE t_distributed_plan_index_analysis (a UInt64, s String)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1024;

INSERT INTO t_distributed_plan_index_analysis SELECT number, toString(number) FROM numbers(200000);
-- One part, so a background merge cannot change the mark count the assertion compares against.
OPTIMIZE TABLE t_distributed_plan_index_analysis FINAL;

-- The settings stay on this query: reading `system.query_log` below cannot be distributed.
-- `max_rows_to_group_by = 0`: distributed aggregation rejects a nonzero limit (randomized setting).
-- `use_query_condition_cache = 0`: a cached verdict would prune granules before analysis and hide how
-- many marks it processes.
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
