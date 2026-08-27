-- Test for a crash in `expandOrderByAll`: a child plan of a `Merge` table re-analyzed the query
-- after `removeJoin` had stripped the ORDER BY clause but left the `order_by_all` flag set.
-- Only the old analyzer is affected.
-- Compared to the original test on master, `NATURAL` joins are rewritten as explicit
-- `USING (a)` joins, because the 25.8 parser does not support `NATURAL JOIN` yet;
-- the produced output is identical.
SET enable_analyzer = 0;

DROP TABLE IF EXISTS t_order_by_all_merge_left;
DROP TABLE IF EXISTS t_order_by_all_merge_right;

CREATE TABLE t_order_by_all_merge_left (a UInt64, s String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_order_by_all_merge_right (a UInt64, t String) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_order_by_all_merge_left VALUES (1, 'q'), (2, 'w');
INSERT INTO t_order_by_all_merge_right VALUES (1, 'r'), (3, 'e');

SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m INNER JOIN t_order_by_all_merge_right AS r USING (a) ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m FULL OUTER JOIN t_order_by_all_merge_right AS r USING (a) ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m FULL OUTER JOIN t_order_by_all_merge_right AS r USING (a) GROUP BY a, s, t ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m FULL OUTER JOIN t_order_by_all_merge_right AS r USING (a) GROUP BY ALL ORDER BY ALL;

-- INTERPOLATE exists in the AST independently of ORDER BY, so it must be removed together with
-- the ORDER BY clause when the JOIN is stripped from a `Merge` child query: otherwise the child
-- query analysis would find an unknown identifier `t` from the removed joined table.
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m FULL OUTER JOIN t_order_by_all_merge_right AS r USING (a) ORDER BY a WITH FILL INTERPOLATE (t AS t);

-- The GROUP BY modifiers must not survive the removal of the GROUP BY clause from a `Merge`
-- child query: a leftover WITH TOTALS/ROLLUP/CUBE/GROUPING SETS flag would make the child
-- query analysis reject the rewritten aggregation-free query.
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m FULL OUTER JOIN t_order_by_all_merge_right AS r USING (a) GROUP BY a, s, t WITH TOTALS ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m FULL OUTER JOIN t_order_by_all_merge_right AS r USING (a) GROUP BY a, s, t WITH ROLLUP ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m FULL OUTER JOIN t_order_by_all_merge_right AS r USING (a) GROUP BY a, s, t WITH CUBE ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m FULL OUTER JOIN t_order_by_all_merge_right AS r USING (a) GROUP BY GROUPING SETS ((a, s, t)) ORDER BY ALL;

-- WINDOW definitions and LIMIT BY expressions are analyzed unconditionally, so they must be
-- removed from a `Merge` child query together with the JOIN: otherwise the child query analysis
-- would find unknown identifiers from the removed joined table.
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m FULL OUTER JOIN t_order_by_all_merge_right AS r USING (a) WINDOW w AS (PARTITION BY t) ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m FULL OUTER JOIN t_order_by_all_merge_right AS r USING (a) ORDER BY ALL LIMIT 1 BY t;

-- LIMIT ... WITH TIES requires an ORDER BY clause, which is removed together with the JOIN from
-- a `Merge` child query, so the flag must be reset as well: a leftover flag would be a logical
-- error in the child query interpreter.
SELECT * FROM merge(currentDatabase(), '^t_order_by_all_merge_left$') AS m FULL OUTER JOIN t_order_by_all_merge_right AS r USING (a) ORDER BY a LIMIT 1 WITH TIES;

DROP TABLE t_order_by_all_merge_left;
DROP TABLE t_order_by_all_merge_right;
