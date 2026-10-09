SET enable_analyzer = 1;
SET allow_experimental_keyed_recursive_cte = 1;

-- Keys are compared exactly, not by a digest: composite string keys whose concatenations are
-- equal, ('a', 'bc') and ('ab', 'c'), stay distinct.
WITH RECURSIVE t USING KEY (a, b) AS
(
    SELECT 'a' AS a, 'bc' AS b, 0 AS v
    UNION ALL
    SELECT 'ab', 'c', v + 1 FROM t WHERE a = 'a' AND v = 0
)
SELECT * FROM t ORDER BY a, b;

-- Reading only the accumulated state `<cte_name>_settled` from the recursive member is a
-- self-reference too.
WITH RECURSIVE t USING KEY (k) AS
(
    SELECT 1 AS k
    UNION ALL
    SELECT k + 1 FROM t_settled WHERE k < 3
)
SELECT * FROM t ORDER BY k;

-- `USING KEY` survives the conversion of the analyzed query tree back to AST.
EXPLAIN SYNTAX run_query_tree_passes = 1
WITH RECURSIVE t USING KEY (k) AS
(
    SELECT 1 AS k
    UNION ALL
    SELECT k + 1 FROM t WHERE k < 3
)
SELECT k FROM t;
