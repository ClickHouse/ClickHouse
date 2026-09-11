-- Tags: no-old-analyzer

CREATE TABLE tiny (tid UInt64) ENGINE = MergeTree ORDER BY tid;
CREATE TABLE mid (tid UInt64) ENGINE = MergeTree ORDER BY tid;
CREATE TABLE huge (hid UInt64, hid2 UInt64, s String) ENGINE = MergeTree ORDER BY hid;
CREATE TABLE big_nullable (bid Nullable(UInt64), v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE small_nullable (sid Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tiny SELECT number * 100 FROM numbers(100);
INSERT INTO mid SELECT number * 10 FROM numbers(10000);
INSERT INTO huge SELECT number, number, toString(number) FROM numbers(1000000);
INSERT INTO big_nullable SELECT number, number FROM numbers(100000);
INSERT INTO small_nullable SELECT number * 100 FROM numbers(100);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET explain_query_plan_default = 'legacy', log_processors_profiles = 1;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
-- Admission of transported filters depends on which relation ends up at each apply site, so pin
-- the join order and the estimate source against test-level randomization.
SET query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0, use_statistics = 0;
SET distributed_plan_join_runtime_filters = 1;

SELECT '-- tiny probe side, huge build side: the filter cannot arrive in time, rows pass unfiltered';
SELECT count() FROM tiny, huge WHERE tid = hid;

SELECT '-- Nullable key is not transportable, the local filter stays';
SELECT count() FROM big_nullable, small_nullable WHERE bid = sid SETTINGS log_comment = '04892_nullable';

SELECT '-- persisted exchanges';
SELECT count() FROM huge, tiny WHERE hid = tid SETTINGS distributed_plan_force_exchange_kind = 'Persisted';

SELECT '-- streaming exchanges';
SELECT count() FROM huge, tiny WHERE hid = tid SETTINGS distributed_plan_force_exchange_kind = 'Streaming';

SELECT '-- early close under LIMIT';
SELECT count() FROM (SELECT hid FROM huge, tiny WHERE hid = tid LIMIT 10);

-- Independent join keys (hid vs hid2) keep the two dimension filters from collapsing into a
-- transitive `mid ⋈ tiny` bushy join. `mid` between `tiny` and `huge` keeps every admission
-- decision far from its estimate threshold, so randomized index-analysis jitter cannot flip the plan.
SELECT '-- two joins, each with its own filter';
SELECT count() FROM huge AS h INNER JOIN mid AS t1 ON h.hid = t1.tid INNER JOIN tiny AS t2 ON h.hid2 = t2.tid
    SETTINGS log_comment = '04892_two_joins';

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- Local distributed-plan tasks inherit `log_comment`, log as `stage_%` / `rf_merge_%` in
-- `system.query_log`, and record their pipeline processors under the initiator's `query_id`.
-- Two things exist only on the transported path and are the transport signal here:
--
--   `BuildRuntimeFilterPartialTransform` serializes a build task's partial and appends it to the
--   task's exchange sink as one extra row, so `output_rows > input_rows` marks a task that put a
--   state on an exchange. It is counted where the state is serialized, so it does not depend on
--   whether anything received it.
--
--   `rf_merge_<bucket>_<filter>` is a task of that filter's merge tree, which is planned only for
--   a transported filter, so the distinct filter names in those task names are the transported
--   filters.
--
-- A filter that stays local is built by `BuildRuntimeFilterTransform` straight into its own
-- task's lookup and produces neither: that transform is present in equal numbers with the setting
-- on and off, which is why it cannot serve as the signal.
--
-- Nothing below asserts on the receiving side. The merge -> probe broadcast is best-effort by
-- design (a probe task cancels its receive branch once its data work is done), so a receive-side
-- count is not a property of this code: at 24 concurrent clients the previous receive-side form
-- of the two-join check failed 20 of 48 runs, and 8 of 36 with `ThreadFuzzer` enabled, while the
-- send-side counts used below were exact in all 48 and all 24 runs of the same conditions.
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

SELECT '-- two joins, each with a transported filter';
-- Two joins means two filters, each with its own merge tree and its own serialized partials: two
-- trees and, at the default bucket count, eight partials each. The tree count stays exact at two
-- - a second join losing its filter, or the two collapsing into one, still fails the check - and
-- the partial count is a bound, so a different bucket count does not break it. A local filter
-- plans no tree and serializes nothing, and scores 0 on both.
SELECT uniqExact(extract(query, '^rf_merge_\\d+_(_runtime_filter_\\d+)')) = 2
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
                     AND current_database = currentDatabase() AND log_comment = '04892_two_joins'))
   ) >= 4
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query LIKE 'rf_merge_%'
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
        AND current_database = currentDatabase() AND log_comment = '04892_two_joins');
