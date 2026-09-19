-- Tags: no-old-analyzer

CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid;
CREATE TABLE small (sid UInt64, name String) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number, number FROM numbers(100000);
INSERT INTO small SELECT number * 100, toString(number) FROM numbers(100);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET explain_query_plan_default = 'legacy', log_processors_profiles = 1;
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

-- Local distributed-plan tasks inherit `log_comment`, log as `stage_%` / `rf_merge_%` in
-- `system.query_log`, and record their pipeline processors under the initiator's `query_id`.
-- Two processors exist only on the transported path and are the transport signal here:
--
--   `BuildRuntimeFilterPartialTransform` serializes a build task's partial and appends it to the
--   task's exchange sink as one extra row, so `output_rows > input_rows` marks a task that put a
--   state on an exchange. It is counted where the state is serialized, so it does not depend on
--   whether anything received it.
--
--   `rf_merge_%` is a task of the merge tree, which is planned only for a transported filter.
--
-- A filter that stays local is built by `BuildRuntimeFilterTransform` straight into its own
-- task's lookup and produces neither: that transform is present in equal numbers with the
-- setting on and off, which is why it cannot serve as the signal.
--
-- Nothing below asserts on the receiving side. The merge -> probe broadcast is best-effort by
-- design (a probe task cancels its receive branch once its data work is done), so a receive-side
-- count is not a property of this code: measured on a busy machine at 24 concurrent clients, the
-- previous receive-side form of these checks failed 44% (shuffle join) and 38% (multiple keys)
-- of the time, while every count used below was exact in 288 out of 288 runs.
SYSTEM FLUSH LOGS query_log, processors_profile_log;

SELECT '-- shuffle join, setting on: states crossed the exchange';
-- 8 build tasks (the `distributed_plan_default_shuffle_join_bucket_count` default), each
-- serializing its partial onto the tree's exchange; >= 2 is the discriminator, because a local
-- filter never leaves its own task and would score 0.
SELECT countIf(name = 'BuildRuntimeFilterPartialTransform' AND output_rows > input_rows) >= 2
FROM system.processors_profile_log
WHERE event_date >= yesterday()
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND event_date >= yesterday()
        AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
              AND current_database = currentDatabase() AND log_comment = '04891_transport_on'));

SELECT '-- broadcast join, setting on: single-task build still ships the filter';
-- The broadcast build side is a single task, so exactly one partial is serialized -- pinned at
-- one, not `>= 1`, so a return to a per-bucket build side would not pass unnoticed.
SELECT countIf(name = 'BuildRuntimeFilterPartialTransform' AND output_rows > input_rows) = 1
FROM system.processors_profile_log
WHERE event_date >= yesterday()
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND event_date >= yesterday()
        AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
              AND current_database = currentDatabase() AND log_comment = '04891_broadcast'));

SELECT '-- multiple keys, setting on: each key is sent';
-- Two join keys means two filters, each with its own merge tree and its own 8 serialized
-- partials: 2 trees and 16 partials, checked as 2 trees with >= 2 partials each.
SELECT uniqExact(extract(query, '^rf_merge_\\d+_(_runtime_filter_\\d+)')) >= 2
   AND (
       SELECT countIf(name = 'BuildRuntimeFilterPartialTransform' AND output_rows > input_rows)
       FROM system.processors_profile_log
       WHERE event_date >= yesterday()
         AND query_id IN (
             SELECT query_id FROM system.query_log
             WHERE type = 'QueryFinish' AND event_date >= yesterday()
               AND initial_query_id IN (
                   SELECT query_id FROM system.query_log
                   WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
                     AND current_database = currentDatabase() AND log_comment = '04891_multi_key'))
   ) >= 4
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query LIKE 'rf_merge_%'
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
        AND current_database = currentDatabase() AND log_comment = '04891_multi_key');

SELECT '-- anti join keeps its local filter: not sent';
-- The control: the anti join's filter must never reach an exchange, so neither transported
-- processor may appear. `count() > 0` keeps it from holding just because no task ran.
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
                  AND current_database = currentDatabase() AND log_comment = '04891_anti_join'))
) = 0
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday()
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
        AND current_database = currentDatabase() AND log_comment = '04891_anti_join')
  AND (query LIKE 'stage_%' OR query LIKE 'rf_merge_%');
