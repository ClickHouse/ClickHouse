-- Tags: no-old-analyzer

-- 20000 distinct short `String` keys under the default 10000-row exact-values limit.
-- Transport must raise the row bound from the cardinality estimate for variable-width
-- keys too, so the filter can arrive exact.

CREATE TABLE big (bid String, v UInt64) ENGINE = MergeTree ORDER BY bid;
CREATE TABLE small (sid String) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT toString(number), number FROM numbers(100000);
INSERT INTO small SELECT toString(number * 5) FROM numbers(20000);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
-- More than one build task, so the exact partials also cross the merge stage of the tree.
SET distributed_plan_default_reader_bucket_count = 4, distributed_plan_default_shuffle_join_bucket_count = 4;
-- The default row cap, pinned: the estimate must overrule it.
SET join_runtime_filter_exact_values_limit = 10000;

SELECT '-- identical results with transported filters off and on';
SELECT count() FROM big, small WHERE bid = sid SETTINGS distributed_plan_join_runtime_filters = 0;
SET distributed_plan_join_runtime_filters = 1;
SELECT count() FROM big, small WHERE bid = sid SETTINGS log_comment = '04517_exact_string_transport';

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log, text_log;

-- Admission trace: exact-values limit must be the 20000 estimated build keys, not the
-- 10000-row settings cap (that would degrade to bloom on row 10001).
SELECT '-- admitted with the estimate-derived row bound';
SELECT count() > 0 FROM system.text_log
WHERE logger_name = 'joinRuntimeFilter'
    AND message LIKE '%admitted%20000 estimated build keys%20000 exact values limit%'
    AND event_date >= yesterday() AND query_id IN (
        SELECT query_id FROM system.query_log
        WHERE type = 'QueryFinish' AND is_initial_query AND log_comment = '04517_exact_string_transport'
            AND current_database = currentDatabase() AND event_date >= yesterday());
