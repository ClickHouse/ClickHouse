-- Tags: no-parallel-replicas

-- When a `view(...)` table function wraps a `remote(...)` subquery, the outer
-- `WHERE` predicate sits above `ReadFromRemote` in the plan and the fallback
-- `addFilters` path in `ReadFromRemote.cpp` is responsible for pushing it into
-- the remote query via `tryBuildAdditionalFilterAST`. The remote query lands
-- as `SELECT ... FROM numbers(3) AS __table1`, where the table expression is a
-- `TableFunctionNode`. Previously `addFilters` only accepted `TableNode`, so
-- the predicate was silently dropped from the remote query.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET prefer_localhost_replica = 1;
SET serialize_query_plan = 0;

-- Correctness: filter should still match the right row on every shard.
SELECT number
FROM view(SELECT number FROM remote('127.0.0.{1,2}', numbers(3)))
WHERE number = 1
ORDER BY number;

-- The remote-side plan (inside `ReadFromRemote`) must include the pushed-down
-- filter. With `distributed = 1` and two shards (one local, one remote), the
-- EXPLAIN should contain three `Filter column: equals(...)` lines:
--   - one above the local `ReadFromSystemNumbers`
--   - one above `ReadFromRemote`
--   - one inside the remote-side plan (this is the line that was missing)
SELECT countIf(explain ILIKE '%Filter column: equals(%')
FROM
(
    EXPLAIN actions = 1, distributed = 1
    SELECT number FROM view(SELECT number FROM remote('127.0.0.{1,2}', numbers(3)))
    WHERE number = 1
);
