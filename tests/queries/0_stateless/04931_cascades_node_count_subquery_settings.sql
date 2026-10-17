-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

DROP TABLE IF EXISTS t1_04931;
DROP TABLE IF EXISTS t2_04931;
CREATE TABLE t1_04931 (key UInt64) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2_04931 (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t1_04931 SELECT number FROM numbers(100);
INSERT INTO t2_04931 SELECT number FROM numbers(100);

-- A scalar subquery is planned with its own settings, so the node count Cascades plans for must come
-- from that plan. A consumer that re-read `distributed_plan_workers_num` or
-- `distributed_plan_execute_locally` from the ambient context saw the outer query's values and
-- planned for the configured worker cluster instead of the requested count.

-- max_rows_to_group_by is reset because the test profile sets it to a nonzero value and distributed
-- aggregation rejects any limit; automatic_parallel_replicas_mode is pinned off alongside
-- enable_parallel_replicas because the test runner randomizes it.

-- The stress profile sets `ast_fuzzer_runs = 5`; a fuzzed re-run inherits `log_comment`, so it would
-- add rows to the lookup below and break the exact Cascades run count.
SET ast_fuzzer_runs = 0;

SELECT (
    SELECT count()
    FROM t1_04931 INNER JOIN t2_04931 ON intDiv(t1_04931.key, 2) = t2_04931.key
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
        distributed_plan_execute_locally = 1, distributed_plan_workers_num = 1,
        enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0
) SETTINGS log_comment = 'one_worker_04931';

SELECT (
    SELECT count()
    FROM t1_04931 INNER JOIN t2_04931 ON intDiv(t1_04931.key, 2) = t2_04931.key
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
        distributed_plan_execute_locally = 1, distributed_plan_workers_num = 4,
        enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0
) SETTINGS log_comment = 'four_workers_04931';

-- Both sides non-zero and different, so the requested count cannot win by coincidence.
SELECT (
    SELECT count()
    FROM t1_04931 INNER JOIN t2_04931 ON intDiv(t1_04931.key, 2) = t2_04931.key
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
        distributed_plan_execute_locally = 1, distributed_plan_workers_num = 4,
        enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0
) SETTINGS distributed_plan_execute_locally = 1, distributed_plan_workers_num = 8,
    log_comment = 'outer_disagrees_04931';

-- Control: the same settings applied to the whole query, so nothing diverges. A blanket change that
-- stopped honoring the requested count would move this one too.
SELECT count()
FROM t1_04931 INNER JOIN t2_04931 ON intDiv(t1_04931.key, 2) = t2_04931.key
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
    distributed_plan_execute_locally = 1, distributed_plan_workers_num = 4,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0,
    log_comment = 'top_level_04931';

-- All four requests resolve from the plan, so the expected counts do not depend on how large the
-- configured worker cluster is. The row count is asserted too: exactly one Cascades run per query.
SYSTEM FLUSH LOGS text_log, query_log;
SET max_rows_to_read = 0; -- system.text_log can be really big

SELECT
    log_comment,
    count() AS cascades_runs,
    any(extract(message, 'cluster node count: (\\d+)')) AS planned_node_count
FROM system.text_log
INNER JOIN (
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('one_worker_04931', 'four_workers_04931', 'outer_disagrees_04931', 'top_level_04931')
    GROUP BY query_id, log_comment
) AS q USING (query_id)
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND logger_name = 'CascadesOptimizer'
    AND message LIKE '%cluster node count:%'
GROUP BY log_comment
ORDER BY log_comment
SETTINGS enable_parallel_replicas = 0;

DROP TABLE t1_04931;
DROP TABLE t2_04931;
