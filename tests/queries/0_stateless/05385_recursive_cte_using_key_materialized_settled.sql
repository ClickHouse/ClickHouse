SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET allow_experimental_keyed_recursive_cte = 1;

-- A MATERIALIZED helper CTE referenced from the recursive member is materialized once, before the
-- recursion starts, so it must not read `<cte_name>_settled` (it would snapshot the empty settled table).
WITH RECURSIVE walk USING KEY (x) AS
(
    SELECT 1 AS x
    UNION ALL
    SELECT walk.x + 1 FROM walk INNER JOIN seen ON seen.x = walk.x WHERE walk.x < 3
),
seen AS MATERIALIZED
(
    SELECT x FROM walk_settled
)
SELECT * FROM walk; -- { serverError UNSUPPORTED_METHOD }

-- Reading `<cte_name>_settled` directly from the recursive member is fine.
WITH RECURSIVE walk USING KEY (k) AS
(
    SELECT 1 AS k
    UNION ALL
    SELECT k + 1 FROM walk WHERE k < 5 AND k + 1 NOT IN (SELECT k FROM walk_settled)
)
SELECT * FROM walk ORDER BY k;
