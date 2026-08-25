-- Test that recursive CTE column types are widened via iterative getLeastSupertype.
-- Without type widening, `x` would be UInt8 and the query would run infinitely due to overflow.

SET enable_analyzer = 1;

WITH RECURSIVE t AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM t)
SELECT x, toTypeName(x) FROM t WHERE x >= 256 LIMIT 1;

-- Verify that explicit cast still works as before.
WITH RECURSIVE t AS (SELECT 1::UInt64 AS x UNION ALL SELECT x + 1 FROM t)
SELECT x, toTypeName(x) FROM t WHERE x >= 256 LIMIT 1;

-- Reach a value above `UInt16` range, which requires more than one widening step.
-- Each `x + 1000` re-resolve advances the type by more than one rank
-- (`UInt8` -> `UInt32` -> `UInt64`), so this case verifies that the iteration
-- continues past the first widening step and converges at `UInt64`. Using
-- a step of 1000 keeps the recursion depth well under
-- `max_recursive_cte_evaluation_depth`.
WITH RECURSIVE t AS (SELECT 1 AS x UNION ALL SELECT x + 1000 FROM t)
SELECT x, toTypeName(x) FROM t WHERE x >= 70000 LIMIT 1;

-- Also test a chain that stays within `UInt16` range to confirm convergence happens early
-- when no further widening is needed.
WITH RECURSIVE t AS (SELECT 1::UInt16 AS x UNION ALL SELECT (x + 1)::UInt16 FROM t WHERE x < 10)
SELECT max(x), toTypeName(max(x)) FROM t;

-- With the cap set to 1, only one widening step is allowed: `UInt8` -> `UInt16`.
-- The final column type stays `UInt16` even though further widening would be needed.
SET recursive_cte_max_steps_in_type_inference = 1;
WITH RECURSIVE t AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM t WHERE x < 10)
SELECT max(x), toTypeName(max(x)) FROM t;

-- With type inference disabled (setting = 0), the type should be `UInt8` (old behavior).
-- The query should still work but `x` will overflow and never reach 256, so we test with a small value.
SET recursive_cte_max_steps_in_type_inference = 0;
WITH RECURSIVE t AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM t WHERE x < 10)
SELECT max(x), toTypeName(max(x)) FROM t;
