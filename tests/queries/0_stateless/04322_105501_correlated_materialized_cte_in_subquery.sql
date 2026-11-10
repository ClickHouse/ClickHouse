SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_materialized_cte = 1;

-- A MATERIALIZED CTE materializes once and cannot depend on outer-scope columns,
-- so its body cannot be a correlated subquery. The analyzer rejects this in the
-- join-tree path (`FROM t`); these cases reach the materialized CTE via the
-- identifier-resolution path (`x IN (t)`), which must reject it the same way.
-- Here `zeros(N)`/`primes(N)` expose columns `zero`/`prime`, so the inner
-- `number` resolves against the outer `numbers(...)`, making the CTE correlated.

WITH t AS MATERIALIZED (SELECT number, number + 65535 FROM zeros(8))
SELECT * FROM numbers(8) WHERE (number + 3, number + 1, number + 3) IN (t); -- { serverError UNSUPPORTED_METHOD }

WITH t AS MATERIALIZED (SELECT number FROM primes(5))
SELECT * FROM numbers(8) WHERE number IN (t); -- { serverError UNSUPPORTED_METHOD }

-- Same correlated CTE under the optimizer settings the fuzzer combined with the
-- crash query (these steer the plan through the set-building paths).
SET enable_join_runtime_filters = 0;
SET query_plan_merge_filter_into_join_condition = 0;
SET query_plan_merge_filters = 0;
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;
WITH t AS MATERIALIZED (SELECT number FROM primes(5))
SELECT * FROM numbers(8) WHERE number IN (t); -- { serverError UNSUPPORTED_METHOD }
SET enable_join_runtime_filters = 1;
SET query_plan_merge_filter_into_join_condition = 1;
SET query_plan_merge_filters = 1;
SET query_plan_convert_any_join_to_semi_or_anti_join = 0;

-- A non-correlated MATERIALIZED CTE used the same way must still work: the guard
-- only rejects correlation, it does not break valid materialized CTEs in IN.
WITH t AS MATERIALIZED (SELECT number FROM numbers(5))
SELECT number FROM numbers(8) WHERE number IN (t) ORDER BY number;
