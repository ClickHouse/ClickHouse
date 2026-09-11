-- Tags: no-fasttest, no-old-analyzer
-- no-fasttest: needs the streaming exchange of the stateless worker configuration.
-- no-old-analyzer: distributed planning requires the analyzer.

-- The sending side of a streaming exchange keeps its work spread over the streams of a task: every
-- scattered stream serializes its own packets ahead of the sinks. The checks read the processors of
-- every task from `processors_profile_log`. They are relations between counts, not exact counts, so
-- they hold for any stream count. When a check fails, the last query prints the layout of the tasks
-- behind it.

DROP TABLE IF EXISTS t_exchange_layout;
CREATE TABLE t_exchange_layout (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
SYSTEM STOP MERGES t_exchange_layout;
INSERT INTO t_exchange_layout SELECT number, number FROM numbers(2000);
INSERT INTO t_exchange_layout SELECT number, number FROM numbers(2000, 2000);
INSERT INTO t_exchange_layout SELECT number, number FROM numbers(4000, 2000);
INSERT INTO t_exchange_layout SELECT number, number FROM numbers(6000, 2000);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0;
SET distributed_plan_default_reader_bucket_count = 1, distributed_plan_default_shuffle_join_bucket_count = 3;
-- Four parts read on four streams, so there is work to spread.
SET max_threads = 4, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1;
SET log_processors_profiles = 1, explain_query_plan_default = 'legacy';

-- The plan the checks run against: a scatter out of the reader task and a gather into the main
-- task. A change here explains a change in the layout below.
EXPLAIN SELECT sum(v) FROM t_exchange_layout;
SELECT sum(v) FROM t_exchange_layout SETTINGS log_comment = '05176_distributed_plan_exchange_layout';

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- The exchange processors of every task of the query above. The query id of a task is the id of
-- the query followed by `::` and the stage.
CREATE VIEW v_exchange_layout AS
SELECT query_id, substring(query_id, position(query_id, '::') + 2) AS stage,
    countIf(name = 'ScatterByPartitionTransform') AS scatters,
    countIf(name LIKE 'StreamingExchangeSink%') AS sinks,
    countIf(name = 'StreamingExchangeSerializingTransform') AS serializers,
    maxIf(input_rows, name = 'StreamingExchangeSerializingTransform') AS max_rows_into_serializer,
    maxIf(input_rows, name = 'ScatterByPartitionTransform') AS max_rows_into_scatter,
    countIf(name LIKE 'StreamingExchangeSource%') AS sources
FROM system.processors_profile_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND query_id LIKE '%::%'
  AND initial_query_id IN (
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND current_database = currentDatabase()
      AND log_comment = '05176_distributed_plan_exchange_layout' AND type = 'QueryFinish')
GROUP BY query_id;

SELECT check, ok FROM (
    SELECT 1 AS step, 'tasks with a scatter and tasks with sources were logged' AS check, countIf(scatters > 0) > 0 AND countIf(sources > 0) > 0 AS ok FROM v_exchange_layout
    UNION ALL SELECT 2, 'scatter: serializers = scatters * sinks', min(serializers = scatters * sinks) FROM v_exchange_layout WHERE scatters > 0
    UNION ALL SELECT 3, 'scatter: max rows into one serializer <= max rows into one scatter', min(max_rows_into_serializer <= max_rows_into_scatter) FROM v_exchange_layout WHERE scatters > 0
) ORDER BY step;

-- Empty when every check passes. Otherwise the layout of the tasks behind the failure, with the
-- query id to find the task in the server log.
SELECT format('{} ({}): scatters={} sinks={} serializers={} max_rows_into_serializer={} max_rows_into_scatter={} sources={}',
    stage, query_id, toString(scatters), toString(sinks), toString(serializers), toString(max_rows_into_serializer),
    toString(max_rows_into_scatter), toString(sources))
FROM v_exchange_layout
WHERE scatters > 0 AND (serializers != scatters * sinks OR max_rows_into_serializer > max_rows_into_scatter)
ORDER BY stage;

DROP VIEW v_exchange_layout;
DROP TABLE t_exchange_layout;
