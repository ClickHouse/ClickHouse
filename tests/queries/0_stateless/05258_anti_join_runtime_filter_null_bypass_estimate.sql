-- Tags: no-old-analyzer

-- A `LEFT ANTI JOIN` runtime filter on a `Nullable` key is `__applyFilter(...) OR isNull(key)`.
-- Like a bare `__applyFilter`, it prunes nothing at plan time, so it must not erase the row
-- estimate of the scan below it. The `GROUP BY` under the join must then get the same distributed
-- aggregation plan with and without runtime filters.

SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
-- Without statistics the estimate comes from the primary index. A plan-time filter that the index
-- does not use leaves no estimate.
SET use_statistics = 0;
SET max_rows_to_group_by = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1;
SET distributed_plan_optimize_exchanges = 1;
SET distributed_plan_force_exchange_kind = 'Streaming';
SET distributed_plan_max_rows_to_broadcast = 100000;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET join_runtime_filter_min_probe_rows = 0;

DROP TABLE IF EXISTS probe;
DROP TABLE IF EXISTS build;
CREATE TABLE probe (k Nullable(UInt64), v UInt64) ENGINE = MergeTree ORDER BY v SETTINGS auto_statistics_types = '';
CREATE TABLE build (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO probe SELECT if(number % 97 = 0, NULL, number % 1000), number FROM numbers(10000);
INSERT INTO build SELECT number FROM numbers(10);

SELECT '-- runtime filters off';
SELECT trimLeft(explain) FROM (
    EXPLAIN SELECT count() FROM (SELECT k FROM probe GROUP BY k) AS a LEFT ANTI JOIN build AS b ON a.k = b.k
    SETTINGS enable_join_runtime_filters = 0)
WHERE explain LIKE '%Aggregat%' OR explain LIKE '%Exchange%';

SELECT '-- runtime filters on';
SELECT trimLeft(explain) FROM (
    EXPLAIN SELECT count() FROM (SELECT k FROM probe GROUP BY k) AS a LEFT ANTI JOIN build AS b ON a.k = b.k
    SETTINGS enable_join_runtime_filters = 1)
WHERE explain LIKE '%Aggregat%' OR explain LIKE '%Exchange%';

SELECT '-- the filter is the NULL bypass form';
SELECT count() FROM (
    EXPLAIN actions = 1 SELECT count() FROM (SELECT k FROM probe GROUP BY k) AS a LEFT ANTI JOIN build AS b ON a.k = b.k
    SETTINGS enable_join_runtime_filters = 1, make_distributed_plan = 0)
WHERE explain LIKE '%Filter column: or(__applyFilter(%, isNull(%';

SELECT '-- results';
SELECT count() FROM (SELECT k FROM probe GROUP BY k) AS a LEFT ANTI JOIN build AS b ON a.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT count() FROM (SELECT k FROM probe GROUP BY k) AS a LEFT ANTI JOIN build AS b ON a.k = b.k SETTINGS enable_join_runtime_filters = 1;

DROP TABLE probe;
DROP TABLE build;
