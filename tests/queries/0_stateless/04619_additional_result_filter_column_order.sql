-- Regression test: a multi-column filter compiled through `analyzeExpressionToActionsDAG`
-- must not reorder the result columns. The Analyzer registers DAG inputs in expression
-- first-use order, while the legacy `ExpressionAnalyzer` kept them in source-column order;
-- `FilterStep` materializes the header from the DAG outputs, so with a filter `b > a` over
-- header [a, b, c] the first-use order produced [b, a, c].

DROP TABLE IF EXISTS t_filter_order;
DROP TABLE IF EXISTS t_filter_order_merge;

CREATE TABLE t_filter_order (a UInt8, b UInt8, c UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_filter_order VALUES (1, 2, 3), (5, 4, 6);

-- A row policy over a `Merge` table is compiled by that helper (`ReadFromMerge::RowPolicyData`);
-- the result order must stay [a, b, c].
CREATE TABLE t_filter_order_merge (a UInt8, b UInt8, c UInt8) ENGINE = Merge(currentDatabase(), '^t_filter_order$');
CREATE ROW POLICY 04619_filter_order_policy ON t_filter_order FOR SELECT USING b > a TO ALL;

SELECT * FROM t_filter_order_merge ORDER BY a FORMAT TSVWithNames;

DROP ROW POLICY 04619_filter_order_policy ON t_filter_order;

-- The same when the policy skips over a column (`USING c > a` over header [a, b, c]):
-- the filter DAG lists only the referenced columns, but the converting step restores
-- the unreferenced column in its source position, so the result stays [a, b, c].
CREATE ROW POLICY 04619_filter_order_policy_skip ON t_filter_order FOR SELECT USING c > a TO ALL;

SELECT * FROM t_filter_order_merge ORDER BY a FORMAT TSVWithNames;

DROP ROW POLICY 04619_filter_order_policy_skip ON t_filter_order;
DROP TABLE t_filter_order_merge;
DROP TABLE t_filter_order;
