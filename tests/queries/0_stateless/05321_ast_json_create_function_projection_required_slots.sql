-- `formatQueryFromJSON` rejects a `CREATE FUNCTION` or projection `SELECT` payload that lacks a required slot.

SELECT formatQueryFromJSON(parseQueryToJSON('CREATE FUNCTION f AS (x) -> x + 1'));
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE FUNCTION f AS (x) -> x + 1'), '"function_name"', '"no_function_name"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE FUNCTION f AS (x) -> x + 1'), '"function_core"', '"no_function_core"')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ADD PROJECTION p (SELECT a ORDER BY a)'));
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t ADD PROJECTION p (SELECT a ORDER BY a)'), '"select"', '"no_select"')); -- { serverError BAD_ARGUMENTS }
