-- Tags: no-old-analyzer

-- A recursive CTE evaluates its steps by building a whole new InterpreterSelectQueryAnalyzer per
-- iteration, from RecursiveCTESource::generate -- that is, while the outer query is already
-- running. If such an interpreter can reach the query's plan profiler, it captures over the plan
-- this row is about, and `query_plan` ends up describing the recursive arm instead. The profiler
-- is therefore handed only to the interpreter executeQuery built, and nested ones never see it.
--
-- The check is the plan's own `Output`: the outer query selects `sum(n)`, every recursive step
-- selects `n`. Asserting on that rather than on step names keeps the test independent of how the
-- recursion is planned.

SET log_query_plans = 1;
WITH RECURSIVE counter AS
(
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM counter WHERE n < 5
)
SELECT sum(n) FROM counter SETTINGS log_comment = '05182_recursive_cte' FORMAT Null;
SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    'recursive_cte',
    count(),
    -- Present and parseable at all.
    anyLast(isValidJSON(toJSONString(query_plan))),
    -- The stored plan is the one whose output is the aggregate, not the recursive step's `n`.
    anyLast(position(arrayStringConcat(JSONExtractArrayRaw(toJSONString(query_plan), 'Output')), 'sum')) > 0,
    -- And the root it names really is a node of the same document.
    anyLast(arrayExists(
        n -> JSONExtractString(n, 'Node Id') = JSONExtractString(toJSONString(query_plan), 'Root'),
        JSONExtractArrayRaw(toJSONString(query_plan), 'Nodes')))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05182_recursive_cte';
