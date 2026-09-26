DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (x Int32) ENGINE = Memory;
INSERT INTO t0 VALUES (1);

-- The original problematic query pattern - inner CTE references outer CTE
-- Using count() to get deterministic output regardless of how many rows are produced before hitting the limit
SET max_recursive_cte_evaluation_depth = 5;
SET enable_analyzer = 1;

SELECT count() > 0 FROM (
  WITH RECURSIVE q AS (
    SELECT 1 FROM t0 UNION ALL
      (WITH RECURSIVE x AS
        (SELECT 1 FROM t0 UNION ALL
          (SELECT 1 FROM q WHERE FALSE UNION ALL
           SELECT 1 FROM x WHERE FALSE))
         SELECT 1 FROM x))
    SELECT 1 FROM q
); -- { serverError TOO_DEEP_RECURSION }

DROP TABLE t0;
