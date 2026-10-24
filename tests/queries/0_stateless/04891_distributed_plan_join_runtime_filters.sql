-- Tags: no-old-analyzer

CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE small (sid UInt64, name String) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number, number FROM numbers(100000);
INSERT INTO small SELECT number * 100, toString(number) FROM numbers(100);

SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
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

-- Tasks of a local distributed plan log under their own `query_id`, with the initiator's id as
-- `initial_query_id`. Signs that a filter was sent:
--   * `RuntimeFilterStatesSent` of the build tasks (`stage_%`): a task made its filter state for
--     the exchange. It is counted before the send, so it does not depend on whether the merge task
--     still reads (it stops when all probe tasks are done).
--   * an `rf_merge_%` task: it exists only for a sent filter.
-- A local filter shows neither. The receiver is not checked: a probe task stops waiting for the
-- filter when its data work is done, so the filter may not arrive.
SYSTEM FLUSH LOGS query_log, text_log, processors_profile_log;

SELECT '-- shuffle join, setting on: states crossed the exchange';
-- 8 build tasks (the `distributed_plan_default_shuffle_join_bucket_count` default), each
-- serializing its partial onto the tree's exchange; >= 2 is the discriminator, because a local
-- filter never leaves its own task and would score 0.
SELECT sum(ProfileEvents['RuntimeFilterStatesSent']) >= 2
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query LIKE 'stage_%'
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
        AND current_database = currentDatabase() AND log_comment = '04891_transport_on');

SELECT '-- broadcast join, setting on: single-task build still ships the filter';
-- The broadcast build side is a single task, so exactly one partial is serialized -- pinned at
-- one, not `>= 1`, so a return to a per-bucket build side would not pass unnoticed.
SELECT sum(ProfileEvents['RuntimeFilterStatesSent']) = 1
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query LIKE 'stage_%'
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
        AND current_database = currentDatabase() AND log_comment = '04891_broadcast');

SELECT '-- multiple keys, setting on: each key is sent';
-- Two join keys means two filters, each with its own merge tree and its own 8 serialized
-- partials: 2 trees and 16 partials, checked as at least 2 trees and at least 4 partials.
SELECT uniqExact(extract(query, '^rf_merge_\\d+_(_runtime_filter_\\d+)')) >= 2
   AND (
       SELECT sum(ProfileEvents['RuntimeFilterStatesSent'])
       FROM system.query_log
       WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query LIKE 'stage_%'
         AND initial_query_id IN (
             SELECT query_id FROM system.query_log
             WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
               AND current_database = currentDatabase() AND log_comment = '04891_multi_key')
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

SELECT '-- anti join keeps its local filter: registered and applied';
-- The check above shows that nothing was sent, not that a filter still works. A filter that is
-- not transported works only inside the join stage: each join task derives its own local
-- build/apply pair when it optimizes its fragment. The initiator's build step sits in the
-- build-scan stage with no apply site next to it and does nothing. So a `Registered runtime filter`
-- line comes from a join task's local build. A `Stats for` line with a nonzero block count comes
-- from the apply site that found that filter.
SELECT countIf(startsWith(message, 'Registered runtime filter')) >= 1
   AND countIf(startsWith(message, 'Stats for')
       AND toUInt64OrZero(extract(message, 'blocks processed (\\d+)'))
           + toUInt64OrZero(extract(message, 'blocks skipped (\\d+)')) > 0) >= 1
FROM system.text_log
WHERE event_date >= yesterday() AND logger_name = 'RuntimeFilter'
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND event_date >= yesterday()
        AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
              AND current_database = currentDatabase() AND log_comment = '04891_anti_join'));
