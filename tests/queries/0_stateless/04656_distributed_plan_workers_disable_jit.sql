-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- make_distributed_plan auto-disables compile_expressions (issue #109476), including on worker
-- tasks, whose contexts are rebuilt from the initiator's user-level settings and would otherwise
-- keep the default. The setting is enabled explicitly here on purpose: the override is
-- unconditional, because a worker cannot honor it regardless of how it was set.
-- use_skip_indexes_on_data_read is NOT overridden anymore: it propagates to workers as set,
-- because direct reads from a text index depend on it (see 04836_text_index_direct_read_make_distributed_plan).

DROP TABLE IF EXISTS t_dp_big;
DROP TABLE IF EXISTS t_dp_small;

CREATE TABLE t_dp_big (id UInt64, v UInt64, INDEX idx_v v TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_dp_small (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dp_big SELECT number, number FROM numbers(100000);
INSERT INTO t_dp_small SELECT number * 100 FROM numbers(1000);

SET make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_default_shuffle_join_bucket_count = 3, max_rows_to_group_by = 0,
    enable_join_runtime_filters = 0;

SET use_skip_indexes_on_data_read = 1, compile_expressions = 1, min_count_to_compile_expression = 0;
SET log_comment = '04656_distributed_plan_auto_switch';

SELECT 'join with a skip-index filter and a JIT-eligible expression matches single-node';
SELECT count(), sum(b.v + 1) FROM t_dp_big AS b INNER JOIN t_dp_small AS s ON b.v = s.id
    WHERE b.v < 50000;
SELECT count(), sum(b.v + 1) FROM t_dp_big AS b INNER JOIN t_dp_small AS s ON b.v = s.id
    WHERE b.v < 50000
    SETTINGS make_distributed_plan = 0;

SELECT 'worker tasks run with JIT disabled and skip-index reads propagated';
SYSTEM FLUSH LOGS query_log;
-- Worker task entries log the task id ('main', 'stage_N_M' - not SQL) as the query text and share
-- the initiator's query_id; their Settings column shows the task context, where the overrides
-- appear as changed settings. Scoping to the query_id of the latest 'main' task makes the probe
-- immune to earlier runs of this test in the same database, and the task-id filter excludes the
-- initiator row itself (logged before the auto-switch, so it keeps the user-set values). The probe
-- runs with make_distributed_plan = 0 so it does not spawn 'main' tasks of its own.
SELECT DISTINCT Settings['use_skip_indexes_on_data_read'], Settings['compile_expressions']
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryStart'
  AND query NOT ILIKE '%SELECT%'
  AND query_id = (
      SELECT query_id FROM system.query_log
      WHERE event_date >= yesterday() AND type = 'QueryStart'
        AND current_database = currentDatabase()
        AND Settings['log_comment'] = '04656_distributed_plan_auto_switch'
        AND query = 'main'
      ORDER BY event_time_microseconds DESC
      LIMIT 1)
SETTINGS make_distributed_plan = 0;

DROP TABLE t_dp_big;
DROP TABLE t_dp_small;
