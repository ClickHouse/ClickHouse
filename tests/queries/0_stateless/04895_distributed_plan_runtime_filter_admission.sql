-- Tags: no-old-analyzer

CREATE TABLE t_small (sid UInt64) ENGINE = MergeTree ORDER BY sid;
CREATE TABLE t_large (lid UInt64) ENGINE = MergeTree ORDER BY lid;
INSERT INTO t_small SELECT number * 100 FROM numbers(100);
INSERT INTO t_large SELECT number FROM numbers(1000000);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET explain_query_plan_default = 'legacy', log_processors_profiles = 1;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_join_runtime_filters = 1;

-- More estimated build keys than the probe site has rows: shipping the filter costs at least as
-- much as it could ever save, so transport is refused and the local build step stays.
SELECT '-- build side larger than the probe site: transport refused';
SELECT count() FROM t_small, t_large WHERE sid = lid SETTINGS log_comment = '04895_refused';

SELECT '-- small build side against a large probe site: transport admitted';
SELECT count() FROM t_large, t_small WHERE lid = sid SETTINGS log_comment = '04895_admitted';

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log, text_log, processors_profile_log;

-- The admission decision is taken while the plan is optimized, on the initiator, and is read
-- from `system.text_log` below. Whether a state was then sent is read from
-- `system.processors_profile_log` instead: `BuildRuntimeFilterPartialTransform` serializes a
-- build task's partial and appends it to the task's exchange sink as one extra row, so
-- `output_rows > input_rows` marks a task that put a state on an exchange, and
-- `MergeRuntimeFiltersTransform` belongs to a merge-tree task. Both exist only on the transported
-- path; a filter that stays local is built by `BuildRuntimeFilterTransform`, which appears in
-- equal numbers whether transport is admitted or refused and so cannot serve as the signal.
--
-- These counts are taken where the state is serialized, not where it arrives. The merge -> probe
-- broadcast is best-effort by design (a probe task cancels its receive branch once its data work
-- is done), so an arrival count is not a property of this code and cannot be asserted.
SELECT '-- refused: admission trace and no states sent';
SELECT
    (
        SELECT count() > 0 FROM system.text_log
        WHERE logger_name = 'joinRuntimeFilter' AND message LIKE '%refused at%'
            AND event_date >= yesterday() AND query_id IN (
                SELECT query_id FROM system.query_log
                WHERE type = 'QueryFinish' AND is_initial_query AND log_comment = '04895_refused'
                    AND current_database = currentDatabase() AND event_date >= yesterday())
    ),
    -- A refused filter must never reach an exchange, so neither transported processor may appear.
    -- This one stays an expected zero: it separates a refused filter from the admitted one below,
    -- which scores 8 on the same count, not a transported filter from a local one.
    -- `count() > 0` keeps it from holding just because no task ran.
    (
        SELECT count() > 0 AND (
            SELECT count()
            FROM system.processors_profile_log
            WHERE event_date >= yesterday()
              AND name IN ('BuildRuntimeFilterPartialTransform', 'MergeRuntimeFiltersTransform')
              AND query_id IN (
                  SELECT query_id FROM system.query_log
                  WHERE type = 'QueryFinish' AND event_date >= yesterday()
                    AND initial_query_id IN (
                        SELECT query_id FROM system.query_log
                        WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
                          AND current_database = currentDatabase() AND log_comment = '04895_refused'))
        ) = 0
        FROM system.query_log
        WHERE type = 'QueryFinish' AND event_date >= yesterday()
          AND initial_query_id IN (
              SELECT query_id FROM system.query_log
              WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
                AND current_database = currentDatabase() AND log_comment = '04895_refused')
          AND (query LIKE 'stage_%' OR query LIKE 'rf_merge_%')
    );

SELECT '-- admitted: admission trace and states sent';
SELECT
    (
        SELECT count() > 0 FROM system.text_log
        WHERE logger_name = 'joinRuntimeFilter' AND message LIKE '%admitted at%'
            AND event_date >= yesterday() AND query_id IN (
                SELECT query_id FROM system.query_log
                WHERE type = 'QueryFinish' AND is_initial_query AND log_comment = '04895_admitted'
                    AND current_database = currentDatabase() AND event_date >= yesterday())
    ),
    -- One serialized partial per build task, eight of them at the default bucket count; `>= 2`
    -- is the discriminator, because a local filter never leaves its own task and scores 0.
    (
        SELECT countIf(name = 'BuildRuntimeFilterPartialTransform' AND output_rows > input_rows) >= 2
        FROM system.processors_profile_log
        WHERE event_date >= yesterday()
          AND query_id IN (
              SELECT query_id FROM system.query_log
              WHERE type = 'QueryFinish' AND event_date >= yesterday()
                AND initial_query_id IN (
                    SELECT query_id FROM system.query_log
                    WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
                      AND current_database = currentDatabase() AND log_comment = '04895_admitted'))
    );
