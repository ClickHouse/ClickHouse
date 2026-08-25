CREATE TABLE test
(
    `key` UInt64,
    `value` Int64
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO test SELECT cityHash64(number) AS key, number AS value FROM numbers(100);

SET enable_parallel_replicas = 0;
SET enable_analyzer = 1;
SET enable_join_runtime_filters = 0;
SET query_plan_enable_multithreading_after_window_functions = 0; -- inserts extra Expression node after window step, changing EXPLAIN output
SET query_plan_reuse_storage_ordering_for_window_functions = 1; -- when False, planner inserts extra Expression node due to inability to reuse storage ordering
SET query_plan_optimize_join_order_limit = 10; -- CI may inject 0, causing chooseJoinOrder to be skipped, leaving intermediate unnamed Expression steps un-eliminated
SET query_plan_remove_unused_columns = 1; -- CI may inject False; "Discarding unused columns" step before GROUP BY is omitted, changing the Expression step label in EXPLAIN output
EXPLAIN PLAN
WITH
    view_1 AS
    (
        SELECT
            key,
            ROW_NUMBER() OVER (PARTITION BY key) AS rn
        FROM test
    ),
    view_2 AS
    (
        SELECT
            key,
            count() > 0 AS has_any
        FROM test
        GROUP BY
            key
    ),
    events AS
    (
        SELECT
            *
        FROM view_1 AS v1
        INNER JOIN view_2 AS v2_1 USING (key)
        LEFT JOIN view_2 AS v2_2 USING (key)
        WHERE v1.rn = 1
    )
SELECT count()
FROM events
WHERE v2_1.has_any;
