SET query_plan_optimize_join_order_randomize = 0; -- Pinned because the test asserts on join plan/order
SET query_plan_join_swap_table = false;
SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas=0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- Disable automatic spilling for this test

-- Test that with default join_algorithm setting, we are doing a parallel hash join

SELECT value == 'direct,hash,ie_join' FROM system.settings WHERE name = 'join_algorithm';

SELECT
    *
FROM
    (
        SELECT * FROM system.numbers LIMIT 100000
    ) t1
    JOIN
    (
        SELECT * FROM system.numbers LIMIT 100000
    ) t2
USING number
SETTINGS max_threads=16, log_comment='03274_default_setting'
FORMAT Null;

-- Test that join_algorithm = default also does a parallel hash join

SET join_algorithm='default';

SELECT value == 'default' FROM system.settings WHERE name = 'join_algorithm';

SELECT
    *
FROM
    (
        SELECT * FROM system.numbers LIMIT 100000
    ) t1
    JOIN
    (
        SELECT * FROM system.numbers LIMIT 100000
    ) t2
USING number
SETTINGS max_threads=16, log_comment='03274_default_value'
FORMAT Null;

SET join_algorithm=DEFAULT; -- reset

-- Check that compat setting also achieves a parallel hash join

SET compatibility='24.11';

SELECT
    *
FROM
    (
        SELECT * FROM system.numbers LIMIT 100000
    ) t1
    JOIN
    (
        SELECT * FROM system.numbers LIMIT 100000
    ) t2
USING number
SETTINGS max_threads=16, log_comment='03274_compatibility'
FORMAT Null;

-- The right side has no size estimate, so each join builds with the parallel layout
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('03274_default_setting', '03274_default_value', '03274_compatibility')
ORDER BY event_time_microseconds;
