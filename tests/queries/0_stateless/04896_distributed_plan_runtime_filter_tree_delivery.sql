-- Tags: no-old-analyzer

CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid;
CREATE TABLE small (sid UInt64, name String) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number, number FROM numbers(1000000);
INSERT INTO small SELECT number * 100, toString(number) FROM numbers(10000);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_join_runtime_filters = 1;
-- More than one build task, so the partials go through the bounded merge tree instead of
-- all-to-all delivery (the bucket count is kept even for tiny tables, see
-- `setupDistributedReadBuckets`).
SET distributed_plan_default_reader_bucket_count = 4, distributed_plan_default_shuffle_join_bucket_count = 4;

SELECT '-- tree delivery, streaming exchange';
SELECT count() FROM big, small WHERE bid = sid SETTINGS log_comment = '04516_tree_streaming';

SELECT '-- tree delivery, persisted exchange';
SELECT count() FROM big, small WHERE bid = sid
SETTINGS log_comment = '04516_tree_persisted', distributed_plan_force_exchange_kind = 'Persisted';

SET make_distributed_plan = 0;

SYSTEM FLUSH LOGS query_log, text_log;

-- Every variant must have scheduled and finished a merge-tree stage task (`rf_merge_*`), and the
-- probe-scan tasks (stage_0) must have registered the union that arrived through the tree: a probe
-- task holds only the filter delivered over the exchange, and it cannot finish before consuming
-- the filter stream, so its `RuntimeFilter` log line proves end-to-end tree delivery.
SELECT '-- merge stage ran and the union was registered on the probe tasks';
SELECT
    comment,
    countIf(query LIKE 'rf_merge_%') > 0 AS merge_stage_ran,
    countIf(query LIKE 'stage_0_%' AND has_registration) > 0 AS probe_registered
FROM
(
    SELECT tasks.query AS query, initiators.log_comment AS comment, tasks.query_id IN (
        SELECT query_id FROM system.text_log
        WHERE logger_name = 'RuntimeFilter' AND event_date >= yesterday()) AS has_registration
    FROM system.query_log AS tasks
    INNER JOIN
    (
        SELECT query_id, log_comment FROM system.query_log
        WHERE type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '04516_tree_%'
            AND current_database = currentDatabase() AND event_date >= yesterday()
    ) AS initiators ON tasks.initial_query_id = initiators.query_id
    WHERE tasks.type = 'QueryFinish' AND tasks.event_date >= yesterday()
)
GROUP BY comment
ORDER BY comment;
