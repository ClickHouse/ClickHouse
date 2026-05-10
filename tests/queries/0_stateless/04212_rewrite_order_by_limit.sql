-- Regression test for `RewriteOrderByLimitPass` correctness with `ORDER BY ... LIMIT ... OFFSET`.
-- The rewrite splits the query into a subquery (sort key + part offsets) and a self-join that
-- re-reads wide columns by row position. `OFFSET` must be cleared on the outer query so it is
-- not applied a second time on top of the subquery's `LIMIT/OFFSET`.

DROP TABLE IF EXISTS t_rewrite_order_by_limit;

CREATE TABLE t_rewrite_order_by_limit (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_rewrite_order_by_limit SELECT number, number * 10 FROM numbers(20);

-- Lower the column-count gate so the rewrite fires for this narrow test table.
SET query_plan_min_columns_to_use_rewrite_order_by_limit = 1;

-- LIMIT only (baseline).
SELECT 'limit' AS section;
SELECT * FROM t_rewrite_order_by_limit ORDER BY k LIMIT 5 SETTINGS query_plan_rewrite_order_by_limit = 0;
SELECT * FROM t_rewrite_order_by_limit ORDER BY k LIMIT 5 SETTINGS query_plan_rewrite_order_by_limit = 1;

-- LIMIT with OFFSET.
SELECT 'limit_offset' AS section;
SELECT * FROM t_rewrite_order_by_limit ORDER BY k LIMIT 5 OFFSET 3 SETTINGS query_plan_rewrite_order_by_limit = 0;
SELECT * FROM t_rewrite_order_by_limit ORDER BY k LIMIT 5 OFFSET 3 SETTINGS query_plan_rewrite_order_by_limit = 1;

-- LIMIT n, m form (offset = n, limit = m).
SELECT 'limit_n_m' AS section;
SELECT * FROM t_rewrite_order_by_limit ORDER BY k LIMIT 3, 5 SETTINGS query_plan_rewrite_order_by_limit = 0;
SELECT * FROM t_rewrite_order_by_limit ORDER BY k LIMIT 3, 5 SETTINGS query_plan_rewrite_order_by_limit = 1;

-- DESC with OFFSET.
SELECT 'desc_offset' AS section;
SELECT * FROM t_rewrite_order_by_limit ORDER BY k DESC LIMIT 4 OFFSET 2 SETTINGS query_plan_rewrite_order_by_limit = 0;
SELECT * FROM t_rewrite_order_by_limit ORDER BY k DESC LIMIT 4 OFFSET 2 SETTINGS query_plan_rewrite_order_by_limit = 1;

DROP TABLE t_rewrite_order_by_limit;
