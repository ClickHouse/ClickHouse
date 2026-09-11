-- Tags: no-old-analyzer

CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid;
CREATE TABLE small_build (sid UInt64) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number, number FROM numbers(4000000);
INSERT INTO small_build SELECT number * 1000 FROM numbers(100);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET log_processors_profiles = 1;
SET distributed_plan_join_runtime_filters = 1;

-- The build side is another stage's output - an aggregate behind its own shuffle - so
-- `BuildRuntimeFilter` is placed above an exchange rather than on a bare table scan. That is the
-- case where the producing stage is itself downstream of a cut, so the filter is built with no
-- table to estimate from and the receiving stage is one the producing stage depends on.
--
-- Forcing the shape needs the legacy planner: under `enable_cascades_optimizer` this query gets a
-- `Broadcast HashJoin` in one stage with nothing transported at all, and
-- `param__internal_join_table_stat_hints` does not steer that decision - inverting the two
-- cardinalities by a factor of four million leaves the plan byte-identical.
SELECT count() FROM big JOIN (SELECT sid FROM small_build GROUP BY sid) AS s ON big.bid = s.sid
WHERE v < 90000
SETTINGS log_comment = '04950_delivery_from_join_stage';

SET make_distributed_plan = 0;

SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- Read the send side, not the arrivals. `BuildRuntimeFilterPartialTransform` appends a serialized
-- partial to its task's exchange sink as one extra row, exists only on the transported path, and
-- is counted where the state is serialized - so it does not depend on whether a probe task was
-- still running to receive it. A filter that stayed local would score 0.
SELECT '-- the join-stage build shipped its partials';
SELECT countIf(name = 'BuildRuntimeFilterPartialTransform' AND output_rows > input_rows) >= 2
FROM system.processors_profile_log
WHERE event_date >= yesterday()
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND event_date >= yesterday()
        AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
              AND current_database = currentDatabase()
              AND log_comment = '04950_delivery_from_join_stage'));

-- One merge tree was planned for the one filter. A merge stage exists only for a transported
-- filter, so this also fails if the plan collapses back to a local build.
SELECT '-- one merge tree was planned';
SELECT countIf(query LIKE 'rf_merge_%') = 1
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday()
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
        AND current_database = currentDatabase()
        AND log_comment = '04950_delivery_from_join_stage');
