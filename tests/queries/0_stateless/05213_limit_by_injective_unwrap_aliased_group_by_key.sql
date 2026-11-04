-- Unwrapping an injective function in a `LIMIT BY` key stops at a `GROUP BY` key even when the two spellings
-- of the key carry different aliases (a positional argument copies the projection together with its alias).

DROP TABLE IF EXISTS t_limit_by_aliased_key;
CREATE TABLE t_limit_by_aliased_key (c0 Int32, c1 Int32) ENGINE = Memory;
INSERT INTO t_limit_by_aliased_key VALUES (1, 10), (1, 20), (2, 30);

SET enable_positional_arguments = 1;
SET optimize_injective_functions_in_limit_by = 1;
SET group_by_use_nulls = 0;

-- The GROUP BY key stays toString(c0) under ROLLUP, CUBE and TOTALS.
SELECT toString(c0) AS s, sum(c1) AS m FROM t_limit_by_aliased_key GROUP BY toString(c0) WITH ROLLUP ORDER BY s, m LIMIT 1 BY 1;
SELECT toString(c0) AS s, sum(c1) AS m FROM t_limit_by_aliased_key GROUP BY toString(c0) WITH CUBE ORDER BY s, m LIMIT 1 BY 1;
SELECT toString(c0) AS s, sum(c1) AS m FROM t_limit_by_aliased_key GROUP BY toString(c0) WITH TOTALS ORDER BY s, m LIMIT 1 BY 1;
SELECT toString(c0) AS s, c1 FROM t_limit_by_aliased_key GROUP BY GROUPING SETS ((toString(c0)), (c1)) ORDER BY s, c1 LIMIT 1 BY 1;
-- Plain GROUP BY with its own unwrapping disabled.
SELECT toString(c0) AS s, sum(c1) AS m FROM t_limit_by_aliased_key GROUP BY toString(c0) ORDER BY s, m LIMIT 1 BY 1 SETTINGS optimize_injective_functions_in_group_by = 0;
-- The stale alias is on a nested node: the outer toString is unwrapped down to the key toString(c0)...
SELECT toString(toString(c0) AS y) AS s, sum(c1) AS m FROM t_limit_by_aliased_key GROUP BY toString(c0) WITH ROLLUP ORDER BY s, m LIMIT 1 BY 1;
-- ... and no further.
SELECT countIf(endsWith(explain, 'BY toString(__table1.c0) AS y')) FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT toString(toString(c0) AS y) AS s FROM t_limit_by_aliased_key GROUP BY toString(c0) WITH ROLLUP LIMIT 1 BY 1);
-- A self-join column of the same table matches a key of the other side; keeping it is still correct.
SELECT toString(t2.c0) AS s, count() AS n FROM t_limit_by_aliased_key AS t1 INNER JOIN t_limit_by_aliased_key AS t2 ON t1.c1 = t2.c1 GROUP BY toString(t1.c0), t2.c0 WITH ROLLUP ORDER BY s, n LIMIT 1 BY s;

SET group_by_use_nulls = 1;

-- The key carries an alias that its Nullable copy in LIMIT BY keeps.
SELECT g, sum(c1) AS m FROM t_limit_by_aliased_key GROUP BY toString(c0) AS g WITH ROLLUP ORDER BY g, m LIMIT 1 BY toString(c0);

DROP TABLE t_limit_by_aliased_key;
