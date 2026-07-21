-- Regression test: a multi-column filter compiled through `analyzeExpressionToActionsDAG`
-- must not reorder the result columns. The Analyzer registers DAG inputs in expression
-- first-use order, while the legacy `ExpressionAnalyzer` kept them in source-column order;
-- `FilterStep` materializes the header from the DAG outputs, so with a filter `b > a` over
-- header [a, b, c] the first-use order produced [b, a, c].

DROP TABLE IF EXISTS t_filter_order;
DROP TABLE IF EXISTS t_filter_order_merge;

CREATE TABLE t_filter_order (a UInt8, b UInt8, c UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_filter_order VALUES (1, 2, 3), (5, 4, 6);

-- `additional_result_filter` under the legacy interpreter: the filter references columns
-- in an order different from the header.
SELECT * FROM t_filter_order ORDER BY a FORMAT TSVWithNames
SETTINGS additional_result_filter = 'b > a', enable_analyzer = 0;

-- The columns referenced by the filter must keep their source order also when the filter
-- skips over a column (legacy appends unreferenced columns after the referenced ones).
SELECT * FROM t_filter_order ORDER BY a FORMAT TSVWithNames
SETTINGS additional_result_filter = 'c > a', enable_analyzer = 0;

-- Row policy over a `Merge` table goes through the same non-projecting helper
-- (`ReadFromMerge::RowPolicyData`); the result order must stay [a, b, c] in both
-- analyzer modes.
CREATE TABLE t_filter_order_merge (a UInt8, b UInt8, c UInt8) ENGINE = Merge(currentDatabase(), '^t_filter_order$');
CREATE ROW POLICY 04619_filter_order_policy ON t_filter_order FOR SELECT USING b > a TO ALL;

SELECT * FROM t_filter_order_merge ORDER BY a FORMAT TSVWithNames;
SELECT * FROM t_filter_order_merge ORDER BY a FORMAT TSVWithNames SETTINGS enable_analyzer = 0;

DROP ROW POLICY 04619_filter_order_policy ON t_filter_order;
DROP TABLE t_filter_order_merge;
DROP TABLE t_filter_order;
