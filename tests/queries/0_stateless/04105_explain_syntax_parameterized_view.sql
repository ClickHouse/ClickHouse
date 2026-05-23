-- https://github.com/ClickHouse/ClickHouse/issues/103249
-- EXPLAIN SYNTAX should inline parameterized view calls with their parameter-substituted
-- inner queries, so users can see what the view expands to.

DROP TABLE IF EXISTS 04105_join_target;
DROP VIEW IF EXISTS 04105_pv;
DROP VIEW IF EXISTS 04105_pv_multi;
DROP VIEW IF EXISTS 04105_pv_nested_inner;
DROP VIEW IF EXISTS 04105_pv_nested_outer;
DROP VIEW IF EXISTS 04105_plain_view;
DROP VIEW IF EXISTS numbers;

CREATE TABLE 04105_join_target (number UInt64) ENGINE = Memory;
CREATE VIEW 04105_pv AS SELECT number FROM numbers({n:UInt64}) WHERE number > 10;
CREATE VIEW 04105_pv_multi AS SELECT number FROM numbers({n:UInt64}) WHERE number > {m:UInt64};
CREATE VIEW 04105_pv_nested_inner AS SELECT number FROM numbers({n:UInt64});
CREATE VIEW 04105_pv_nested_outer AS SELECT number FROM 04105_pv_nested_inner(n = {n:UInt64}) WHERE number > 0;
CREATE VIEW 04105_plain_view AS SELECT 1 AS x;

-- Basic parameterized view expansion.
EXPLAIN SYNTAX SELECT * FROM 04105_pv(n = 15);

-- Multiple parameters.
EXPLAIN SYNTAX SELECT * FROM 04105_pv_multi(n = 20, m = 5);

-- Nested parameterized views: each level should be expanded.
EXPLAIN SYNTAX SELECT * FROM 04105_pv_nested_outer(n = 5);

-- Plain (non-parameterized) view is unaffected.
EXPLAIN SYNTAX SELECT * FROM 04105_plain_view;

-- Subquery in outer query references a parameterized view.
EXPLAIN SYNTAX SELECT * FROM (SELECT number FROM 04105_pv(n = 12));

-- Parameterized view in JOIN position should also be expanded.
EXPLAIN SYNTAX SELECT * FROM 04105_join_target JOIN 04105_pv(n = 10) USING number;

-- Explicit alias on the parameterized view must be preserved so the outer query
-- can reference columns through the alias (e.g. `t.number`).
EXPLAIN SYNTAX SELECT t.number FROM 04105_pv(n = 10) AS t;

-- A parameterized view must not shadow a registered table function. With a view
-- named `numbers`, `numbers(3)` still has to resolve to the built-in table
-- function (which is what regular execution does), not be expanded as a view.
CREATE VIEW numbers AS SELECT {n:UInt64} AS x;
EXPLAIN SYNTAX SELECT * FROM numbers(3);
DROP VIEW numbers;

DROP TABLE 04105_join_target;
DROP VIEW 04105_pv;
DROP VIEW 04105_pv_multi;
DROP VIEW 04105_pv_nested_outer;
DROP VIEW 04105_pv_nested_inner;
DROP VIEW 04105_plain_view;
