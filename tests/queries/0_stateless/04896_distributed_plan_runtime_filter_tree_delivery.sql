-- Tags: no-old-analyzer

CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE small (sid UInt64, name String) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number, number FROM numbers(1000000);
INSERT INTO small SELECT number * 100, toString(number) FROM numbers(10000);

SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_join_runtime_filters = 1;
-- Four build tasks, so more than one partial goes into the merge tree (the bucket count is kept
-- even for tiny tables, see `setupDistributedReadBuckets`).
SET distributed_plan_default_reader_bucket_count = 4, distributed_plan_default_shuffle_join_bucket_count = 4;

SELECT '-- tree delivery, streaming exchange';
SELECT count() FROM big, small WHERE bid = sid SETTINGS log_comment = '04896_tree_streaming';

SELECT '-- tree delivery, persisted exchange';
SELECT count() FROM big, small WHERE bid = sid
SETTINGS log_comment = '04896_tree_persisted', distributed_plan_force_exchange_kind = 'Persisted';

SET make_distributed_plan = 0;

SYSTEM FLUSH LOGS query_log;

-- Each variant must run an `rf_merge_%` task, and more than one build task (`stage_%`) must make
-- its filter state for it; this is what makes the delivery a tree. `RuntimeFilterStatesSent` is
-- counted before the send, so it does not depend on whether the merge task still reads: it stops
-- when all probe tasks are done. `>= 2` does not depend on the exact bucket count.
SELECT '-- merge stage ran and the partials went through the tree';
SELECT
    roots.log_comment AS comment,
    countIf(tasks.query LIKE 'rf_merge_%') > 0 AS merge_stage_ran,
    sumIf(tasks.ProfileEvents['RuntimeFilterStatesSent'], tasks.query LIKE 'stage_%') >= 2 AS partials_shipped
FROM system.query_log AS tasks
INNER JOIN
(
    SELECT DISTINCT query_id, log_comment FROM system.query_log
    WHERE type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '04896_tree_%'
        AND current_database = currentDatabase() AND event_date >= yesterday()
) AS roots ON tasks.initial_query_id = roots.query_id
WHERE tasks.type = 'QueryFinish' AND tasks.event_date >= yesterday()
GROUP BY comment
ORDER BY comment;
