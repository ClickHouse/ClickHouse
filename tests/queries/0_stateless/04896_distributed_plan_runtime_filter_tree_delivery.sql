-- Tags: no-old-analyzer

CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE small (sid UInt64, name String) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number, number FROM numbers(1000000);
INSERT INTO small SELECT number * 100, toString(number) FROM numbers(10000);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET log_processors_profiles = 1;
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

SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- Each variant must run an `rf_merge_%` task and send it more than one partial state; this is
-- what makes the delivery a tree. Each of the four build tasks sends its state as one extra row
-- of `BuildRuntimeFilterPartialTransform`; `>= 2` does not depend on the exact bucket count.
-- The receiver is not checked: a probe task stops waiting for the filter when its data work is
-- done, so the filter may not arrive.
SELECT '-- merge stage ran and the partials went through the tree';
SELECT
    comment,
    countIf(kind = 'rf_merge_task') > 0 AS merge_stage_ran,
    countIf(kind = 'shipped_partial') >= 2 AS partials_shipped
FROM
(
    SELECT initiators.log_comment AS comment, 'rf_merge_task' AS kind
    FROM system.query_log AS tasks
    INNER JOIN
    (
        -- One row per variant: the initiator's own row. The task rows join to it through
        -- `initial_query_id`.
        SELECT DISTINCT query_id, log_comment FROM system.query_log
        WHERE type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '04896_tree_%'
            AND current_database = currentDatabase() AND event_date >= yesterday()
    ) AS initiators ON tasks.initial_query_id = initiators.query_id
    WHERE tasks.type = 'QueryFinish' AND tasks.event_date >= yesterday()
        AND tasks.query LIKE 'rf_merge_%'
    UNION ALL
    SELECT initiators.log_comment AS comment, 'shipped_partial' AS kind
    FROM system.processors_profile_log AS partials
    INNER JOIN
    (
        -- The partials are recorded under the build tasks' own `query_id`s; the tasks' rows tie
        -- them to the variant through `initial_query_id`.
        SELECT DISTINCT tasks.query_id AS query_id, roots.log_comment AS log_comment
        FROM system.query_log AS tasks
        INNER JOIN
        (
            SELECT DISTINCT query_id, log_comment FROM system.query_log
            WHERE type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '04896_tree_%'
                AND current_database = currentDatabase() AND event_date >= yesterday()
        ) AS roots ON tasks.initial_query_id = roots.query_id
        WHERE tasks.type = 'QueryFinish' AND tasks.event_date >= yesterday()
    ) AS initiators ON partials.query_id = initiators.query_id
    WHERE partials.event_date >= yesterday()
        AND partials.name = 'BuildRuntimeFilterPartialTransform'
        AND partials.output_rows > partials.input_rows
)
GROUP BY comment
ORDER BY comment;
