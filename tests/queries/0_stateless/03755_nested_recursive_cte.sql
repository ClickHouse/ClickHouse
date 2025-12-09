DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (x Int32) ENGINE = Memory;
INSERT INTO t0 VALUES (1);

-- This query has an outer recursive CTE (q) containing an inner recursive CTE (x)
-- where the inner CTE references both itself (x) and the outer CTE (q).
-- The query is intentionally infinitely recursive, so we limit the recursion depth.
SET max_recursive_cte_evaluation_depth = 5;

WITH RECURSIVE q AS (
  SELECT 1 FROM t0 UNION ALL
    (WITH RECURSIVE x AS
      (SELECT 1 FROM t0 UNION ALL
        (SELECT 1 FROM q WHERE FALSE UNION ALL
         SELECT 1 FROM x WHERE FALSE))
       SELECT 1 FROM x))
  SELECT 1 FROM q; -- { serverError TOO_DEEP_RECURSION }

DROP TABLE t0;
