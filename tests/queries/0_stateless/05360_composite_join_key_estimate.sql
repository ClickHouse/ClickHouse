-- A join on two columns matches pairs of values. Taking only the most selective column counts every
-- sale of a ticket for each of its returns: 200000 rows, 100 times too many here. Counting the pairs,
-- each side having at most as many as rows and at most the product of the NDVs of its key columns,
-- gives 2000, exact here but too few when the key columns are correlated. The estimate is the
-- geometric mean of the two, 20000, and it is marked as resting on an assumption (`from defaults`).
-- Only the estimate of the join is checked: the rest of the plan varies with settings the runner
-- randomizes.
SET allow_experimental_statistics = 1;
SET use_statistics = 1;
-- The test runner may turn this off; the inserted parts need their statistics.
SET materialize_statistics_on_insert = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;

DROP TABLE IF EXISTS t_ck_sales;
DROP TABLE IF EXISTS t_ck_returns;

CREATE TABLE t_ck_sales (item UInt64 STATISTICS(uniq, basic), ticket UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY ticket SETTINGS auto_statistics_types = '';
CREATE TABLE t_ck_returns (item UInt64 STATISTICS(uniq, basic), ticket UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY ticket SETTINGS auto_statistics_types = '';
-- Every (item, ticket) pair is one sale; every tenth sale is returned.
INSERT INTO t_ck_sales SELECT number % 100, intDiv(number, 100) FROM numbers(20000);
INSERT INTO t_ck_returns SELECT number % 100, intDiv(number, 100) FROM numbers(20000) WHERE number % 10 = 3;

SELECT 'true rows', count() FROM t_ck_sales AS s JOIN t_ck_returns AS r ON s.item = r.item AND s.ticket = r.ticket;

SELECT '-- two key columns';
SELECT replaceRegexpAll(regexpExtract(explain, '\\(rows: .*\\)$'), ', cost: [0-9.e+]+', '')
FROM (
    EXPLAIN estimates = 1, bounds = 1
    SELECT count() FROM t_ck_sales AS s JOIN t_ck_returns AS r ON s.item = r.item AND s.ticket = r.ticket
)
WHERE explain LIKE '%Join (%';

SELECT '-- a column in two keys: the second key is not independent, only the most selective key counts';
SELECT replaceRegexpAll(regexpExtract(explain, '\\(rows: .*\\)$'), ', cost: [0-9.e+]+', '')
FROM (
    EXPLAIN estimates = 1, bounds = 1
    SELECT count() FROM t_ck_sales AS s JOIN t_ck_returns AS r ON s.ticket = r.ticket AND s.ticket = r.item
)
WHERE explain LIKE '%Join (%';

DROP TABLE t_ck_sales;
DROP TABLE t_ck_returns;
