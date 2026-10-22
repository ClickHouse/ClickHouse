-- `LIMIT BY` is evaluated after aggregation. Unwrapping an injective function in a `LIMIT BY` key
-- must stop at a `GROUP BY` key: with `GROUP BY c0 + 1`, rewriting `LIMIT BY c0 + 1` to `LIMIT BY c0`
-- asks for a column that no longer exists after the aggregation (NOT_FOUND_COLUMN_IN_BLOCK).

DROP TABLE IF EXISTS t_limit_by_injective;
CREATE TABLE t_limit_by_injective (c0 Int32, c1 Int32) ENGINE = Memory;
INSERT INTO t_limit_by_injective VALUES (1, 10), (1, 20), (2, 30);

SET enable_analyzer = 1;
SET optimize_injective_functions_in_limit_by = 1;

-- The `GROUP BY` key stays `c0 + 1` when its own injective unwrapping is disabled.
SET optimize_injective_functions_in_group_by = 0;

SELECT c0 + 1 AS expr FROM t_limit_by_injective GROUP BY c0 + 1 ORDER BY expr LIMIT 1 BY expr;
SELECT toString(c0) AS s FROM t_limit_by_injective GROUP BY toString(c0) ORDER BY s LIMIT 1 BY s;
SELECT c0 + 1 AS expr, c1 FROM t_limit_by_injective GROUP BY GROUPING SETS ((c0 + 1), (c1)) ORDER BY expr, c1 LIMIT 1 BY expr;
SELECT c0 + 1 AS expr, c1 FROM t_limit_by_injective GROUP BY c0 + 1, c1 WITH ROLLUP ORDER BY expr, c1 LIMIT 1 BY expr;

-- A `LIMIT BY` key that wraps a `GROUP BY` key is unwrapped down to that key and no further.
SELECT toString(c0 + 1) AS s FROM t_limit_by_injective GROUP BY c0 + 1 ORDER BY s LIMIT 1 BY s;
EXPLAIN QUERY TREE SELECT toString(c0 + 1) AS s FROM t_limit_by_injective GROUP BY c0 + 1 ORDER BY s LIMIT 1 BY s;

-- An aggregate wrapped in an injective function is unwrapped to the aggregate, which does exist.
SELECT c0, toString(sum(c1)) AS s FROM t_limit_by_injective GROUP BY c0 ORDER BY c0 LIMIT 1 BY s;

-- With the `GROUP BY` unwrapping enabled, both keys become `c0` and the queries keep working.
SET optimize_injective_functions_in_group_by = 1;

SELECT c0 + 1 AS expr FROM t_limit_by_injective GROUP BY c0 + 1 ORDER BY expr LIMIT 1 BY expr;
SELECT toString(c0 + 1) AS s FROM t_limit_by_injective GROUP BY c0 + 1 ORDER BY s LIMIT 1 BY s;

-- Without aggregation the unwrapping is unrestricted.
SELECT c0 + 1 AS expr, c1 FROM t_limit_by_injective ORDER BY expr, c1 LIMIT 1 BY expr;

-- With `group_by_use_nulls` everything resolved after `GROUP BY` sees the keys promoted to
-- `Nullable`, so the `LIMIT BY` key arrives as a `Nullable` clone of the `GROUP BY` key. Node
-- comparison includes the result type, so that shape has to be recognised as a key too, otherwise
-- the unwrapping walks past it again.
SET group_by_use_nulls = 1;
SET optimize_injective_functions_in_group_by = 0;

SELECT toString(c0) AS s FROM t_limit_by_injective GROUP BY toString(c0) WITH ROLLUP ORDER BY s LIMIT 1 BY s;
SELECT c0 + 1 AS expr FROM t_limit_by_injective GROUP BY c0 + 1 WITH CUBE ORDER BY expr LIMIT 1 BY expr;
SELECT c0 + 1 AS expr, c1 FROM t_limit_by_injective GROUP BY GROUPING SETS ((c0 + 1), (c1)) ORDER BY expr, c1 LIMIT 1 BY expr;

-- The same with the `LIMIT BY` key spelled out instead of referenced through the projection alias.
SELECT toString(c0) AS s FROM t_limit_by_injective GROUP BY toString(c0) WITH ROLLUP ORDER BY s LIMIT 1 BY toString(c0);

-- A `LIMIT BY` key wrapping a `Nullable` `GROUP BY` key is still unwrapped down to that key.
SELECT toString(c0 + 1) AS s FROM t_limit_by_injective GROUP BY c0 + 1 WITH ROLLUP ORDER BY s LIMIT 1 BY s;
EXPLAIN QUERY TREE SELECT toString(c0 + 1) AS s FROM t_limit_by_injective GROUP BY c0 + 1 WITH ROLLUP ORDER BY s LIMIT 1 BY s;

SET group_by_use_nulls = 0;

DROP TABLE t_limit_by_injective;
