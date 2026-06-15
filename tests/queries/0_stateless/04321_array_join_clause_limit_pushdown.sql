-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/82279
-- The construction-time `numbersLikeUtils::shouldPushdownLimit` previously
-- inspected only `query.select()` for `arrayJoin` functions. The clause
-- form `... ARRAY JOIN expr` is stored separately on `ASTSelectQuery::
-- arrayJoinExpressionList` and went through unguarded, so the source got a
-- hard `LIMIT n` cap before the array-join expansion ran. Rows with empty
-- arrays in the truncated prefix produced zero output.

-- Bot's exact repro: rows 0..2 produce empty arrays via `if`, so a source
-- limit of 3 pre-`ARRAY JOIN` truncates to numbers 0..2 and the clause
-- expands no rows. Without the source limit, the source generates enough
-- numbers for `ARRAY JOIN` to produce at least 3 output rows.
SELECT '-- ARRAY JOIN clause: empty-array prefix';
SELECT number FROM numbers(100) ARRAY JOIN if(number < 3, [], [number]) AS x LIMIT 3 SETTINGS allow_experimental_analyzer = 1;
SELECT '-- ARRAY JOIN clause, old analyzer';
SELECT number FROM numbers(100) ARRAY JOIN if(number < 3, [], [number]) AS x LIMIT 3 SETTINGS allow_experimental_analyzer = 0;

-- The same bug shape exercised through `query_plan_enable_optimizations = 0`
-- proves that the pushdown happens at AST/construction time, not in the
-- query-plan optimizer (so this guard is independent of
-- `optimizePrimaryKeyConditionAndLimit` covered separately for the function
-- form in `04320_arrayjoin_no_sort_limit_pushdown`).
SELECT '-- ARRAY JOIN clause, optimizer disabled';
SELECT number FROM numbers(100) ARRAY JOIN if(number < 3, [], [number]) AS x LIMIT 3
SETTINGS query_plan_enable_optimizations = 0;

-- A top-level `WITH` alias whose expression is an `arrayJoin` is normalized
-- into the referencing clauses (`SELECT`/`WHERE`/...) before
-- `shouldPushdownLimit` inspects the AST, so the existing `query.select()`
-- check already sees it. These cases lock that in: the source must NOT be
-- capped to the empty-array prefix.
SELECT '-- WITH alias arrayJoin in SELECT';
WITH arrayJoin(if(number < 3, [], [number])) AS x SELECT x FROM numbers(100) LIMIT 3 SETTINGS allow_experimental_analyzer = 1;
SELECT '-- WITH alias arrayJoin in SELECT, old analyzer';
WITH arrayJoin(if(number < 3, [], [number])) AS x SELECT x FROM numbers(100) LIMIT 3 SETTINGS allow_experimental_analyzer = 0;
SELECT '-- WITH alias arrayJoin in SELECT, optimizer disabled';
WITH arrayJoin(if(number < 3, [], [number])) AS x SELECT x FROM numbers(100) LIMIT 3 SETTINGS query_plan_enable_optimizations = 0;

-- Referenced only in `WHERE`: the alias still expands rows, so pushdown must
-- stay disabled.
SELECT '-- WITH alias arrayJoin in WHERE only';
WITH arrayJoin(if(number < 3, [], [number])) AS x SELECT number FROM numbers(100) WHERE x >= 0 LIMIT 3 SETTINGS allow_experimental_analyzer = 1;
SELECT '-- WITH alias arrayJoin in WHERE only, old analyzer';
WITH arrayJoin(if(number < 3, [], [number])) AS x SELECT number FROM numbers(100) WHERE x >= 0 LIMIT 3 SETTINGS allow_experimental_analyzer = 0;

-- An UNREFERENCED `WITH` alias produces no expansion, so pushdown is still
-- valid and the source is correctly capped. A blanket "reject if `query.with()`
-- contains arrayJoin" check would wrongly disable this; guard against that.
SELECT '-- WITH alias arrayJoin unreferenced: pushdown still valid';
WITH arrayJoin([10, 20, 30]) AS unused SELECT number FROM numbers(100) LIMIT 3 SETTINGS allow_experimental_analyzer = 1;
SELECT '-- WITH alias arrayJoin unreferenced, old analyzer';
WITH arrayJoin([10, 20, 30]) AS unused SELECT number FROM numbers(100) LIMIT 3 SETTINGS allow_experimental_analyzer = 0;
