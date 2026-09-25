-- Tags: no-old-analyzer

CREATE TABLE fact (k1 UInt64, k2 UInt64, v UInt64) ENGINE = MergeTree ORDER BY k1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE dim1 (d1 UInt64) ENGINE = MergeTree ORDER BY d1;
CREATE TABLE dim2 (d2 UInt64, attr UInt64) ENGINE = MergeTree ORDER BY d2;
INSERT INTO fact SELECT number, number, number FROM numbers(1000000);
INSERT INTO dim1 SELECT number * 10 FROM numbers(10000);
INSERT INTO dim2 SELECT number * 100, number FROM numbers(1000);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
-- The plan shape and transported-filter admission depend on the join order and the estimate
-- source, so pin them against test-level randomization.
SET query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0, use_statistics = 0;
SET log_processors_profiles = 1;
SET distributed_plan_join_runtime_filters = 1;

-- The apply sites for both dimension filters sit in the fact-scan fragment, below both shuffle
-- exchanges of the nested joins, so both filters must be wired to those scan tasks rather than
-- one exchange below the owning join.
SELECT '-- nested joins: both filters wired to the fact-scan fragment';
SELECT count() FROM fact INNER JOIN dim1 ON fact.k1 = dim1.d1 INNER JOIN dim2 ON fact.k2 = dim2.d2 WHERE dim2.attr < 100
    SETTINGS log_comment = '04894_nested_placement';
SELECT count() FROM fact INNER JOIN dim1 ON fact.k1 = dim1.d1 INNER JOIN dim2 ON fact.k2 = dim2.d2 WHERE dim2.attr < 100
    SETTINGS distributed_plan_join_runtime_filters = 0;

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log, text_log, processors_profile_log;

-- Each dimension join gets its own filter, so two transported filters mean two merge trees and
-- two sets of serialized partials. The tree count stays exact at two: one dimension losing its
-- filter, or the two collapsing into one, still fails. The partial count is a bound, so a
-- different bucket count does not break it. A local filter plans no tree and serializes nothing.
SELECT '-- both filters transported, each through its own merge tree';
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
                     AND current_database = currentDatabase() AND log_comment = '04894_nested_placement'))
   ) >= 2
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query LIKE 'rf_merge_%'
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
        AND current_database = currentDatabase() AND log_comment = '04894_nested_placement');

-- The executor logs every task's exchange streams (logger `executeDistributedQuery`, message
-- `Task '...' input exchange streams: [...]`). A task that receives a filter lists the broadcast
-- exchange of that filter's `rf_merge_%` root among its inputs. Every task that receives a filter
-- must be in one stage, receive both filters, and have no other input exchange. A task without a
-- data input exchange reads storage, and the only scan here that applies the filters is `fact`.
-- The stage is not found by its read step, whose name differs between table engines.
SELECT '-- both filters are wired to every fact-scan task';
WITH
    (
        SELECT groupUniqArrayArray(extractAll(extract(message, 'output exchange streams: \\[([^\\]]*)\\]'), '(exchange_\\d+)__'))
        FROM system.text_log
        WHERE event_date >= yesterday() AND logger_name = 'executeDistributedQuery'
          AND startsWith(message, 'Task \'rf_merge_')
          AND query_id IN (
              SELECT query_id FROM system.query_log
              WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
                AND current_database = currentDatabase() AND log_comment = '04894_nested_placement')
    ) AS filter_exchanges
SELECT length(filter_exchanges) = 2 AND count() >= 1
   AND uniqExact(extract(message, '^Task \'(stage_\\d+)_')) = 1
   AND min(arraySort(arrayDistinct(extractAll(extract(message, 'input exchange streams: \\[([^\\]]*)\\]'), '(exchange_\\d+)__')))
       = arraySort(filter_exchanges))
FROM system.text_log
WHERE event_date >= yesterday() AND logger_name = 'executeDistributedQuery'
  AND startsWith(message, 'Task \'stage_')
  AND hasAny(extractAll(extract(message, 'input exchange streams: \\[([^\\]]*)\\]'), '(exchange_\\d+)__'), filter_exchanges)
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
        AND current_database = currentDatabase() AND log_comment = '04894_nested_placement');
