SET enable_analyzer = 1;

-- 1. Keyed recursion terminates on CYCLIC graphs without an explicit cycle guard:
-- re-derived identical rows do not change the accumulated state and are not propagated.
DROP TABLE IF EXISTS cyc_edges;
CREATE TABLE cyc_edges (u UInt64, v UInt64) ENGINE = Memory;
INSERT INTO cyc_edges VALUES (1, 2), (2, 3), (3, 1), (3, 4);

WITH RECURSIVE reach USING KEY (node) AS
(
    SELECT 1 :: UInt64 AS node
    UNION ALL
    SELECT e.v AS node FROM reach r INNER JOIN cyc_edges e ON e.u = r.node
)
SELECT node FROM reach ORDER BY node;

-- 2. Shortest path (label-correcting Dijkstra): the accumulated keyed state is exposed
-- as `<cte_name>_settled`, so the recursive member can refute non-improving candidates.
DROP TABLE IF EXISTS w_edges;
CREATE TABLE w_edges (u UInt64, v UInt64, w Float64) ENGINE = Memory;
INSERT INTO w_edges VALUES (1, 2, 1), (2, 3, 1), (1, 3, 5), (3, 4, 1), (1, 4, 10);

WITH RECURSIVE sp USING KEY (node) AS
(
    SELECT 1 :: UInt64 AS node, 0 :: Float64 AS cost
    UNION ALL
    SELECT e.v AS node, s.cost + e.w AS cost
    FROM sp s
    INNER JOIN w_edges e ON e.u = s.node
    LEFT JOIN sp_settled st ON st.node = e.v
    WHERE s.cost + e.w < if(st.node = 0, 1e308, st.cost)
    ORDER BY cost DESC
)
SELECT node, cost FROM sp ORDER BY node;

-- 3. Multiple key columns.
WITH RECURSIVE pairs USING KEY (a, b) AS
(
    SELECT 0 :: UInt64 AS a, 0 :: UInt64 AS b, 0 :: UInt64 AS depth
    UNION ALL
    SELECT (a + 1) % 3 AS a, (b + 2) % 3 AS b, depth + 1 AS depth FROM pairs WHERE depth < 10
)
SELECT a, b FROM pairs ORDER BY a, b;

-- 4. USING KEY requires the CTE to actually be recursive.
WITH RECURSIVE notrec USING KEY (x) AS (SELECT 1 AS x UNION ALL SELECT 2 AS x) SELECT * FROM notrec; -- { serverError BAD_ARGUMENTS }

-- 5. Key columns must exist in the projection.
WITH RECURSIVE r USING KEY (nope) AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM r WHERE x < 3) SELECT * FROM r; -- { serverError BAD_ARGUMENTS }

DROP TABLE cyc_edges;
DROP TABLE w_edges;
