SET enable_analyzer = 1;
SET enable_materialized_cte = 0;

WITH RECURSIVE cte AS MATERIALIZED (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM cte WHERE n < 5
)
SELECT * FROM cte; -- { serverError UNSUPPORTED_METHOD }

SET enable_materialized_cte = 1;

WITH RECURSIVE cte AS MATERIALIZED (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM cte WHERE n < 5
)
SELECT * FROM cte; -- { serverError UNSUPPORTED_METHOD }
