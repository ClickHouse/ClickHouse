SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_materialized_cte = 1;

-- A MATERIALIZED CTE materializes once and cannot depend on outer-scope columns,
-- so its body cannot be a correlated subquery. The analyzer must reject this on every
-- path a materialized CTE can be reached: the identifier-resolution path (`x IN (t)`)
-- and the join-tree path (`FROM t`, including reused references in a JOIN).
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

-- Reused in a JOIN: when a MATERIALIZED CTE is referenced more than once it is not
-- inlined, so each reference is a TableNode whose body subquery is resolved separately.
-- The storage-initializing reference may resolve non-correlated while a later reference
-- resolves correlated; every reference must be rejected, otherwise a surviving PLACEHOLDER
-- (correlated-column marker) reaches ExpressionActions::execute at runtime and aborts the
-- server with "Trying to execute PLACEHOLDER action". Here `a` exposes only `uid`, so
-- `database` in `b` resolves against the outer scope, making `b` correlated.
CREATE TABLE users_04322 (uid Int16) ENGINE = Memory;
INSERT INTO users_04322 VALUES (1), (2), (3);

WITH a AS (SELECT uid FROM users_04322), b AS MATERIALIZED (SELECT database, uid FROM a)
SELECT count() FROM b AS l SEMI LEFT JOIN b AS r ON l.uid = r.uid; -- { serverError UNSUPPORTED_METHOD }

WITH a AS (SELECT uid FROM users_04322), b AS MATERIALIZED (SELECT database, uid FROM a)
SELECT count() FROM b AS l INNER JOIN b AS r ON l.uid = r.uid; -- { serverError UNSUPPORTED_METHOD }

WITH a AS (SELECT uid FROM users_04322), b AS MATERIALIZED (SELECT database, uid FROM a)
SELECT count() FROM b AS l CROSS JOIN b AS r; -- { serverError UNSUPPORTED_METHOD }

-- A non-correlated MATERIALIZED CTE reused in a JOIN must still work.
WITH b AS MATERIALIZED (SELECT uid FROM users_04322)
SELECT count() FROM b AS l SEMI LEFT JOIN b AS r ON l.uid = r.uid;

DROP TABLE users_04322;
