-- Tags: no-old-analyzer

CREATE TABLE t_small (sid UInt64) ENGINE = MergeTree ORDER BY sid;
CREATE TABLE t_large (lid UInt64) ENGINE = MergeTree ORDER BY lid;
INSERT INTO t_small SELECT number * 100 FROM numbers(100);
INSERT INTO t_large SELECT number FROM numbers(1000000);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET explain_query_plan_default = 'legacy';
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_join_runtime_filters = 1;

-- More estimated build keys than the probe site has rows: shipping the filter costs at least as
-- much as it could ever save, so transport is refused and the local build step stays.
SELECT '-- build side larger than the probe site: transport refused';
SELECT count() FROM t_small, t_large WHERE sid = lid SETTINGS log_comment = '04895_refused';

SELECT '-- small build side against a large probe site: transport admitted';
SELECT count() FROM t_large, t_small WHERE lid = sid SETTINGS log_comment = '04895_admitted';

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log, text_log;

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
    (
        SELECT count() > 0 AND (
            SELECT count()
            FROM
            (
                SELECT extract(message, 'under key \'([^\']+)\'') AS filter_key
                FROM system.text_log
                WHERE logger_name = 'RuntimeFilter' AND event_date >= yesterday()
                  AND message LIKE 'Registered runtime filter%'
                  AND query_id IN (
                      SELECT query_id FROM system.query_log
                      WHERE type = 'QueryFinish' AND event_date >= yesterday()
                        AND log_comment = '04895_refused'
                        AND (query LIKE 'stage_%' OR query LIKE 'rf_merge_%'))
                GROUP BY filter_key
                HAVING count() >= 2
            )
        ) = 0
        FROM system.query_log
        WHERE type = 'QueryFinish' AND event_date >= yesterday()
          AND log_comment = '04895_refused'
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
    (
        SELECT count() >= 1
        FROM
        (
            SELECT extract(message, 'under key \'([^\']+)\'') AS filter_key
            FROM system.text_log
            WHERE logger_name = 'RuntimeFilter' AND event_date >= yesterday()
              AND message LIKE 'Registered runtime filter%'
              AND query_id IN (
                  SELECT query_id FROM system.query_log
                  WHERE type = 'QueryFinish' AND event_date >= yesterday()
                    AND log_comment = '04895_admitted'
                    AND (query LIKE 'stage_%' OR query LIKE 'rf_merge_%'))
            GROUP BY filter_key
            HAVING count() >= 2
        )
    );
