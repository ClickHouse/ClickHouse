-- Tags: long

-- Output-byte autopr statistics for queries where a JOIN is the top of the replicas plan.
-- A plain `SELECT <col> FROM a JOIN b` (no aggregation/expression above the join) leaves the
-- physical `JoinStep` as the instrumented top-of-replicas node, exercising
-- `JoinStep::supportsDataflowStatisticsCollection` and the `RuntimeDataflowStatisticsCollector`
-- wired into `JoinStep::updatePipeline`. Without that, a join-topped plan is rejected in
-- `findCorrespondingNodeInSingleNodePlan`, nothing is instrumented, and
-- `RuntimeDataflowStatisticsOutputBytes` stays 0 -- which this test flags as a failure.
--
-- Self-contained (no stateful dataset): the data is deterministic, so the output-byte estimate is
-- reproducible and is validated against hard-coded baselines within a generous factor (as in
-- 03634_autopr_output_bytes_estimation).

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
SET parallel_replicas_prefer_local_join=1;

-- Keep the parallelized side oriented as written (the randomizer may flip this).
SET query_plan_join_swap_table='false';

SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET max_bytes_before_external_sort=0, max_bytes_ratio_before_external_sort=0;
SET max_threads=0;
SET use_query_condition_cache=0;

DROP TABLE IF EXISTS oj_left_tbl;
DROP TABLE IF EXISTS oj_right_tbl;

-- Left (parallelized) side: 10M rows with a varied, poorly-compressible payload, so the join
-- output is large enough to estimate meaningfully and stably.
CREATE TABLE oj_left_tbl (key UInt64, payload String) ENGINE = MergeTree ORDER BY key
AS SELECT number, toString(cityHash64(number)) FROM numbers(10000000);

-- Right side: a subset of keys, so INNER matches a ~2M-row slice while LEFT/RIGHT keep all 10M.
CREATE TABLE oj_right_tbl (key UInt64) ENGINE = MergeTree ORDER BY key
AS SELECT number FROM numbers(2000000);

-- INNER JOIN, join is the top of the replicas plan: ~2M matched payloads.
SELECT t1.payload FROM oj_left_tbl AS t1 INNER JOIN oj_right_tbl AS t2 USING (key) FORMAT Null SETTINGS log_comment='04305_join_inner';

-- LEFT JOIN, join is the top: all 10M left payloads pass through.
SELECT t1.payload FROM oj_left_tbl AS t1 LEFT JOIN oj_right_tbl AS t2 USING (key) FORMAT Null SETTINGS log_comment='04305_join_left';

-- RIGHT JOIN, join is the top: the larger table is on the right (the parallelized side), which
-- exercises the RIGHT-join branch; all 10M payloads pass through.
SELECT t2.payload FROM oj_right_tbl AS t1 RIGHT JOIN oj_left_tbl AS t2 USING (key) FORMAT Null SETTINGS log_comment='04305_join_right';

DROP TABLE oj_left_tbl;
DROP TABLE oj_right_tbl;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Fail if the collected output-byte estimate is missing (0, i.e. the join was not instrumented) or
-- deviates from the recorded baseline by more than 2.5x. Baselines are stable run-to-run because
-- the data is deterministic.
WITH map(
    '04305_join_inner', 39243825,
    '04305_join_left',  196168224,
    '04305_join_right', 199826856) AS expected
SELECT format('{} {} {}', log_comment, output_bytes, expected[log_comment])
FROM (
    SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] AS output_bytes
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - toIntervalMinute(15))
      AND (current_database = currentDatabase()) AND (log_comment LIKE '04305_join_%') AND (type = 'QueryFinish')
    ORDER BY event_time_microseconds
)
WHERE output_bytes = 0
   OR greatest(output_bytes, expected[log_comment]) / nullIf(least(output_bytes, expected[log_comment]), 0) > 2;
