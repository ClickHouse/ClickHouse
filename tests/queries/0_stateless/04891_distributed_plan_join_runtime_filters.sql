-- Tags: no-old-analyzer

CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid;
CREATE TABLE small (sid UInt64, name String) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number, number FROM numbers(100000);
INSERT INTO small SELECT number * 100, toString(number) FROM numbers(100);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET explain_query_plan_default = 'legacy';
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;

SELECT '-- shuffle join, setting off';
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID') FROM (
    EXPLAIN actions=1 SELECT count() FROM big, small WHERE bid = sid
) WHERE explain LIKE '%RuntimeFilter%' OR explain LIKE '%Exchange%' OR explain LIKE '%JoinLogical%' OR explain LIKE '%Filter id%' OR explain LIKE '%__applyFilter%';
SELECT count() FROM big, small WHERE bid = sid;

SELECT '-- shuffle join, setting on';
SET distributed_plan_join_runtime_filters = 1;
SELECT count() FROM big, small WHERE bid = sid SETTINGS log_comment = '04891_transport_on';

SELECT '-- broadcast join, setting on';
SELECT count() FROM big, small WHERE bid = sid SETTINGS distributed_plan_max_rows_to_broadcast = 20000, log_comment = '04891_broadcast';

SELECT '-- multiple keys, setting on';
SELECT count() FROM big, small WHERE bid = sid AND v = sid SETTINGS log_comment = '04891_multi_key';

SELECT '-- empty build side, setting on';
SELECT count() FROM big, small WHERE bid = sid AND name = 'no such name';

SELECT '-- anti join keeps its local filter';
SELECT count() FROM big LEFT ANTI JOIN small ON bid = sid SETTINGS log_comment = '04891_anti_join';

SET make_distributed_plan = 0;

-- Local distributed-plan tasks inherit `log_comment` and log as `stage_%` / `rf_merge_%`. A
-- transported filter is registered under one union key on every consuming task; local execution
-- records those tasks under a single `query_id`, so the same key repeating in `RuntimeFilter`
-- registrations is the transport signal. A local or re-optimized filter uses a fresh key per
-- task, which never repeats.
SYSTEM FLUSH LOGS query_log, text_log;

SELECT '-- shuffle join, setting on: states crossed the exchange';
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
            AND initial_query_id IN (
                SELECT query_id FROM system.query_log
                WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
                  AND current_database = currentDatabase() AND log_comment = '04891_transport_on')
            AND (query LIKE 'stage_%' OR query LIKE 'rf_merge_%'))
    GROUP BY filter_key
    HAVING count() >= 2
);

SELECT '-- broadcast join, setting on: single-task build still ships the filter';
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
            AND initial_query_id IN (
                SELECT query_id FROM system.query_log
                WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
                  AND current_database = currentDatabase() AND log_comment = '04891_broadcast')
            AND (query LIKE 'stage_%' OR query LIKE 'rf_merge_%'))
    GROUP BY filter_key
    HAVING count() >= 2
);

SELECT '-- multiple keys, setting on: each key is sent';
SELECT uniqExact(filter_name) >= 2
FROM
(
    SELECT
        extract(message, 'Registered runtime filter \'([^\']+)\'') AS filter_name,
        extract(message, 'under key \'([^\']+)\'') AS filter_key
    FROM system.text_log
    WHERE logger_name = 'RuntimeFilter' AND event_date >= yesterday()
      AND message LIKE 'Registered runtime filter%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE type = 'QueryFinish' AND event_date >= yesterday()
            AND initial_query_id IN (
                SELECT query_id FROM system.query_log
                WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
                  AND current_database = currentDatabase() AND log_comment = '04891_multi_key')
            AND (query LIKE 'stage_%' OR query LIKE 'rf_merge_%'))
    GROUP BY filter_name, filter_key
    HAVING count() >= 2
);

SELECT '-- anti join keeps its local filter: not sent';
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
                AND initial_query_id IN (
                    SELECT query_id FROM system.query_log
                    WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
                      AND current_database = currentDatabase() AND log_comment = '04891_anti_join')
                AND (query LIKE 'stage_%' OR query LIKE 'rf_merge_%'))
        GROUP BY filter_key
        HAVING count() >= 2
    )
) = 0
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday()
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
        AND current_database = currentDatabase() AND log_comment = '04891_anti_join')
  AND (query LIKE 'stage_%' OR query LIKE 'rf_merge_%');
