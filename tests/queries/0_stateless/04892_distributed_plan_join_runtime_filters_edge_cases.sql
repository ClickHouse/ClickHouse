-- Tags: no-old-analyzer

CREATE TABLE tiny (tid UInt64) ENGINE = MergeTree ORDER BY tid;
CREATE TABLE mid (tid UInt64) ENGINE = MergeTree ORDER BY tid;
CREATE TABLE huge (hid UInt64, hid2 UInt64, s String) ENGINE = MergeTree ORDER BY hid SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE big_nullable (bid Nullable(UInt64), v UInt64) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE small_nullable (sid Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tiny SELECT number * 100 FROM numbers(100);
INSERT INTO mid SELECT number * 10 FROM numbers(10000);
INSERT INTO huge SELECT number, number, toString(number) FROM numbers(1000000);
INSERT INTO big_nullable SELECT number, number FROM numbers(100000);
INSERT INTO small_nullable SELECT number * 100 FROM numbers(100);

SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET log_processors_profiles = 1;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
-- Admission of transported filters depends on which relation ends up at each apply site, so pin
-- the join order and the estimate source against test-level randomization.
SET query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0, use_statistics = 0;
SET distributed_plan_join_runtime_filters = 1;

SELECT '-- tiny probe side, huge build side: the filter cannot arrive in time, rows pass unfiltered';
SELECT count() FROM tiny, huge WHERE tid = hid;

SELECT '-- Nullable key is not transportable, the local filter stays';
SELECT count() FROM big_nullable, small_nullable WHERE bid = sid SETTINGS log_comment = '04892_nullable';

SELECT '-- early close under LIMIT';
SELECT count() FROM (SELECT hid FROM huge, tiny WHERE hid = tid LIMIT 10);

-- Different keys (`hid`, `hid2`) keep the two filters separate. The size of `mid` keeps every
-- admission decision far from its threshold, so random settings cannot change the plan.
SELECT '-- two joins, each with its own filter';
SELECT count() FROM huge AS h INNER JOIN mid AS t1 ON h.hid = t1.tid INNER JOIN tiny AS t2 ON h.hid2 = t2.tid
    SETTINGS log_comment = '04892_two_joins';

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log, text_log, processors_profile_log;

-- Tasks of a local distributed plan log under their own `query_id`, with the initiator's id as
-- `initial_query_id`. Signs that a filter was sent:
--   * `RuntimeFilterStatesSent` of the build tasks (`stage_%`): a task made its filter state for
--     the exchange. It is counted before the send, so it does not depend on whether the merge task
--     still reads (it stops when all probe tasks are done).
--   * an `rf_merge_%` task: it exists only for a sent filter; its name ends with the filter name.
-- A local filter shows neither. The receiver is not checked: a probe task stops waiting for the
-- filter when its data work is done, so the filter may not arrive.
SELECT '-- Nullable key sent no states';
-- A `Nullable` join key is not transportable, so neither transported processor may appear.
-- `count() > 0` keeps this from holding just because no task ran.
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
                  AND current_database = currentDatabase() AND log_comment = '04892_nullable'))
) = 0
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday()
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
        AND current_database = currentDatabase() AND log_comment = '04892_nullable')
  AND (query LIKE 'stage_%' OR query LIKE 'rf_merge_%');

SELECT '-- Nullable key keeps its local filter: registered and applied';
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
              AND current_database = currentDatabase() AND log_comment = '04892_nullable'));

SELECT '-- two joins, each with a transported filter';
-- Two joins means two filters, each with its own merge tree and its own serialized partials: two
-- trees and, at the default bucket count, eight partials each. The tree count stays exact at two
-- - a second join losing its filter, or the two collapsing into one, still fails the check - and
-- the partial count is a bound, so a different bucket count does not break it. A local filter
-- plans no tree and serializes nothing, and scores 0 on both.
SELECT uniqExact(extract(query, '^rf_merge_\\d+_(_runtime_filter_\\d+)')) = 2
   AND (
       SELECT sum(ProfileEvents['RuntimeFilterStatesSent'])
       FROM system.query_log
       WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query LIKE 'stage_%'
         AND initial_query_id IN (
             SELECT query_id FROM system.query_log
             WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
               AND current_database = currentDatabase() AND log_comment = '04892_two_joins')
   ) >= 4
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query LIKE 'rf_merge_%'
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
        AND current_database = currentDatabase() AND log_comment = '04892_two_joins');
