-- Tags: no-old-analyzer

CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid;
CREATE TABLE small (sid UInt64, name String) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number, number FROM numbers(1000000);
INSERT INTO small SELECT number * 100, toString(number) FROM numbers(10000);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET log_processors_profiles = 1;
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

SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- Every variant must have scheduled and finished a merge-tree stage task (`rf_merge_%`) and must
-- have shipped more than one partial into it, which is what makes the delivery a tree rather than
-- all-to-all. Both signals exist only on the transported path.
--
-- `BuildRuntimeFilterPartialTransform` serializes a build task's partial and appends it to the
-- task's exchange sink as one extra row, so `output_rows > input_rows` marks a task that put a
-- state on the tree's exchange; there are four of them here, one per build bucket, and `>= 2`
-- keeps the check off the exact bucket count. A filter that stays local is built by
-- `BuildRuntimeFilterTransform` straight into its own task's lookup and ships nothing.
--
-- Neither signal is on the receiving side. The previous form asked whether the probe-scan tasks
-- carried a `RuntimeFilter` log line, on the premise that a probe task cannot finish before
-- consuming the filter stream. It can: a probe task cancels its receive branch once its data work
-- is done, and a filter that arrives after the scan it would have narrowed has nothing left to
-- serve, so an arrival count is not a property of this code. That form also could not tell the
-- paths apart - with `distributed_plan_execute_locally` every task shares the initiator's
-- `query_id`, so the `stage_0_%` restriction selected nothing and the local path's own log lines
-- satisfied it, which is why that column read 1 with `distributed_plan_join_runtime_filters = 0`.
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
        -- One row per variant: with `distributed_plan_execute_locally` every task of a query is
        -- logged under the initiator's own `query_id` and is also marked `is_initial_query`, so
        -- without `DISTINCT` this side of the join would repeat once per task and multiply both
        -- counts below.
        SELECT DISTINCT query_id, log_comment FROM system.query_log
        WHERE type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '04516_tree_%'
            AND current_database = currentDatabase() AND event_date >= yesterday()
    ) AS initiators ON tasks.initial_query_id = initiators.query_id
    WHERE tasks.type = 'QueryFinish' AND tasks.event_date >= yesterday()
        AND tasks.query LIKE 'rf_merge_%'
    UNION ALL
    SELECT initiators.log_comment AS comment, 'shipped_partial' AS kind
    FROM system.processors_profile_log AS partials
    INNER JOIN
    (
        -- `DISTINCT` for the same reason as above.
        SELECT DISTINCT query_id, log_comment FROM system.query_log
        WHERE type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '04516_tree_%'
            AND current_database = currentDatabase() AND event_date >= yesterday()
    ) AS initiators ON partials.query_id = initiators.query_id
    WHERE partials.event_date >= yesterday()
        AND partials.name = 'BuildRuntimeFilterPartialTransform'
        AND partials.output_rows > partials.input_rows
)
GROUP BY comment
ORDER BY comment;
