-- Tags: no-fasttest, no-old-analyzer
-- no-fasttest: needs the streaming exchange of the stateless worker configuration.
-- no-old-analyzer: distributed planning requires the analyzer.

-- The sending side of a streaming exchange keeps its work spread over the streams of a task: every
-- scattered stream serializes its own packets ahead of the sinks, whether the scatter is round-robin
-- or by key, and a broadcast serializes once per stream, not once per destination. The checks read
-- the processors of every task from `processors_profile_log`. They are relations between counts, not
-- exact counts, so they hold for any stream count. When a check fails, the last query prints the
-- layout of the tasks behind it.

DROP TABLE IF EXISTS t_exchange_layout;
DROP TABLE IF EXISTS t_exchange_layout_small;
CREATE TABLE t_exchange_layout (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
SYSTEM STOP MERGES t_exchange_layout;
INSERT INTO t_exchange_layout SELECT number, number FROM numbers(2000);
INSERT INTO t_exchange_layout SELECT number, number FROM numbers(2000, 2000);
INSERT INTO t_exchange_layout SELECT number, number FROM numbers(4000, 2000);
INSERT INTO t_exchange_layout SELECT number, number FROM numbers(6000, 2000);
-- Small and with statistics, so a join against it broadcasts it. Four parts, so it is read on four streams.
CREATE TABLE t_exchange_layout_small (sid UInt64, s UInt64) ENGINE = MergeTree ORDER BY sid SETTINGS auto_statistics_types = 'basic';
SYSTEM STOP MERGES t_exchange_layout_small;
INSERT INTO t_exchange_layout_small SELECT number, number FROM numbers(25);
INSERT INTO t_exchange_layout_small SELECT number, number FROM numbers(25, 25);
INSERT INTO t_exchange_layout_small SELECT number, number FROM numbers(50, 25);
INSERT INTO t_exchange_layout_small SELECT number, number FROM numbers(75, 25);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0;
SET distributed_plan_default_reader_bucket_count = 1, distributed_plan_default_shuffle_join_bucket_count = 3;
-- Four parts read on four streams, so there is work to spread.
SET max_threads = 4, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1;
SET log_processors_profiles = 1, explain_query_plan_default = 'legacy';

-- The plans the checks run against. A change here explains a change in the layout below.
-- 1. A round-robin scatter out of the reader task and a gather into the main task.
EXPLAIN SELECT sum(v) FROM t_exchange_layout;
SELECT sum(v) FROM t_exchange_layout SETTINGS log_comment = '05176_distributed_plan_exchange_layout_any';
-- 2. A scatter by the hash of the group key.
EXPLAIN SELECT count() FROM (SELECT k % 10, count() FROM t_exchange_layout GROUP BY k % 10) SETTINGS distributed_plan_force_shuffle_aggregation = 1;
SELECT count() FROM (SELECT k % 10, count() FROM t_exchange_layout GROUP BY k % 10)
    SETTINGS distributed_plan_force_shuffle_aggregation = 1, log_comment = '05176_distributed_plan_exchange_layout_hash';
-- 3. A broadcast of the small side of a join.
EXPLAIN SELECT count() FROM t_exchange_layout, t_exchange_layout_small WHERE t_exchange_layout_small.sid = t_exchange_layout.k % 100
    SETTINGS use_statistics = 1, query_plan_join_swap_table = 0, enable_join_runtime_filters = 0;
SELECT count() FROM t_exchange_layout, t_exchange_layout_small WHERE t_exchange_layout_small.sid = t_exchange_layout.k % 100
    SETTINGS use_statistics = 1, query_plan_join_swap_table = 0, enable_join_runtime_filters = 0, log_comment = '05176_distributed_plan_exchange_layout_broadcast';

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- The exchange processors of every task of the queries above. The query id of a task is the id of
-- the query followed by `::` and the stage; the query itself runs on the initiator.
CREATE VIEW v_exchange_layout AS
SELECT substring(roots.log_comment, length('05176_distributed_plan_exchange_layout_') + 1) AS run,
    p.query_id AS query_id,
    if(position(p.query_id, '::') = 0, 'initiator', substring(p.query_id, position(p.query_id, '::') + 2)) AS stage,
    countIf(name LIKE 'MergeTreeSelect%') AS reads,
    sumIf(output_rows, name LIKE 'MergeTreeSelect%') AS rows_read,
    countIf(name = 'ScatterByPartitionTransform') AS scatters,
    countIf(name LIKE 'StreamingExchangeSink%') AS sinks,
    countIf(name = 'StreamingExchangeSerializingTransform') AS serializers,
    sumIf(input_rows, name = 'StreamingExchangeSerializingTransform') AS rows_serialized,
    maxIf(input_rows, name = 'StreamingExchangeSerializingTransform') AS max_rows_into_serializer,
    maxIf(input_rows, name = 'ScatterByPartitionTransform') AS max_rows_into_scatter,
    countIf(name LIKE 'StreamingExchangeSource%') AS sources
FROM system.processors_profile_log AS p
INNER JOIN (
    SELECT query_id, log_comment FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND current_database = currentDatabase()
      AND log_comment IN ('05176_distributed_plan_exchange_layout_any', '05176_distributed_plan_exchange_layout_hash', '05176_distributed_plan_exchange_layout_broadcast')
      AND type = 'QueryFinish') AS roots ON p.initial_query_id = roots.query_id
WHERE p.event_date >= yesterday() AND p.event_time >= now() - INTERVAL 10 MINUTE
GROUP BY run, query_id;

SELECT check, ok FROM (
    SELECT 1 AS step, 'every run logged a task with a scatter and a task with sources' AS check,
        countDistinctIf(run, scatters > 0) = 3 AND countDistinctIf(run, sources > 0) = 3 AS ok FROM v_exchange_layout
    UNION ALL SELECT 2, 'the broadcast run logged a task with several sinks and no scatter', countIf(run = 'broadcast' AND sinks > 1 AND scatters = 0) > 0 FROM v_exchange_layout
    UNION ALL SELECT 3, 'scatter: serializers = scatters * sinks', min(serializers = scatters * sinks) FROM v_exchange_layout WHERE scatters > 0
    UNION ALL SELECT 4, 'scatter: max rows into one serializer <= max rows into one scatter', min(max_rows_into_serializer <= max_rows_into_scatter) FROM v_exchange_layout WHERE scatters > 0
    UNION ALL SELECT 5, 'broadcast: serializers = read streams and serialized rows = read rows', min(serializers = reads AND rows_serialized = rows_read) FROM v_exchange_layout WHERE sinks > 1 AND scatters = 0
) ORDER BY step;

-- Empty when every check passes. Otherwise the layout of the tasks behind the failure, with the
-- query id to find the task in the server log.
SELECT format('{} {} ({}): reads={} rows_read={} scatters={} sinks={} serializers={} rows_serialized={} max_rows_into_serializer={} max_rows_into_scatter={} sources={}',
    run, stage, query_id, toString(reads), toString(rows_read), toString(scatters), toString(sinks), toString(serializers), toString(rows_serialized),
    toString(max_rows_into_serializer), toString(max_rows_into_scatter), toString(sources))
FROM v_exchange_layout
WHERE (scatters > 0 AND (serializers != scatters * sinks OR max_rows_into_serializer > max_rows_into_scatter))
   OR (sinks > 1 AND scatters = 0 AND (serializers != reads OR rows_serialized != rows_read))
ORDER BY run, stage;

DROP VIEW v_exchange_layout;
DROP TABLE t_exchange_layout;
DROP TABLE t_exchange_layout_small;
