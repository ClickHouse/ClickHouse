-- Test for a crash in `expandOrderByAll`: a child plan of a `Merge` table re-analyzed the query
-- after `removeJoin` had stripped the ORDER BY clause but left the `order_by_all` flag set.
-- Only the old analyzer is affected.
SET enable_analyzer = 0;

DROP TABLE IF EXISTS t_order_by_all_merge_left;
DROP TABLE IF EXISTS t_order_by_all_merge_right;

CREATE TABLE t_order_by_all_merge_left (a UInt64, s String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_order_by_all_merge_right (a UInt64, t String) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_order_by_all_merge_left VALUES (1, 'q'), (2, 'w');
INSERT INTO t_order_by_all_merge_right VALUES (1, 'r'), (3, 'e');

SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m NATURAL INNER JOIN t_order_by_all_merge_right AS r ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m NATURAL FULL OUTER JOIN t_order_by_all_merge_right AS r ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m NATURAL FULL OUTER JOIN t_order_by_all_merge_right AS r GROUP BY a, s, t ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m NATURAL FULL OUTER JOIN t_order_by_all_merge_right AS r GROUP BY ALL ORDER BY ALL;

-- INTERPOLATE exists in the AST independently of ORDER BY, so it must be removed together with
-- the ORDER BY clause when the JOIN is stripped from a `Merge` child query: otherwise the child
-- query analysis would find an unknown identifier `t` from the removed joined table.
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m NATURAL FULL OUTER JOIN t_order_by_all_merge_right AS r ORDER BY a WITH FILL INTERPOLATE (t AS t);

-- The GROUP BY modifiers must not survive the removal of the GROUP BY clause from a `Merge`
-- child query: a leftover WITH TOTALS/ROLLUP/CUBE/GROUPING SETS flag would make the child
-- query analysis reject the rewritten aggregation-free query.
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m NATURAL FULL OUTER JOIN t_order_by_all_merge_right AS r GROUP BY a, s, t WITH TOTALS ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m NATURAL FULL OUTER JOIN t_order_by_all_merge_right AS r GROUP BY a, s, t WITH ROLLUP ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m NATURAL FULL OUTER JOIN t_order_by_all_merge_right AS r GROUP BY a, s, t WITH CUBE ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m NATURAL FULL OUTER JOIN t_order_by_all_merge_right AS r GROUP BY GROUPING SETS ((a, s, t)) ORDER BY ALL;

DROP TABLE t_order_by_all_merge_left;
DROP TABLE t_order_by_all_merge_right;
