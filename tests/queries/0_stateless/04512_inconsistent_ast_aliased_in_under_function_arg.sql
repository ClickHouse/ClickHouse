-- Issue #110223: an aliased IN whose left operand also has an alias, nested inside
-- a function-argument list, formatted non-idempotently and aborted the server with
-- an "Inconsistent AST formatting" LOGICAL_ERROR (debug/sanitizer builds).
--
-- First format emitted (((x AS a1) IN [y]) AS c3): the alias wrap in
-- ASTWithAlias::formatImpl opened one pair of parens but left current_function set,
-- so the descendant IN operator added a second isolating pair. The re-parse marked
-- the IN parenthesized, and the second format dropped the inner pair -> different AST.
--
-- The fix clears current_function in the alias-wrap branch (mirroring
-- decideParensEmission), so the descendant IN sees it is already isolated.

-- The original reproducer must not abort the server (which column is unknown /
-- what error fires is irrelevant here).
SELECT ifNull(sum((x AS a1) IN [y] AS c3 = TRUE), 0); -- { serverError UNKNOWN_IDENTIFIER }

-- format(format(q)) == format(q): the internal AST round-trip check, from SQL.
SELECT formatQuerySingleLine('SELECT ifNull(sum((x AS a1) IN [y] AS c3 = TRUE), 0)') =
       formatQuerySingleLine(formatQuerySingleLine('SELECT ifNull(sum((x AS a1) IN [y] AS c3 = TRUE), 0)'));

-- The canonical (stable) formatted form.
SELECT formatQuerySingleLine('SELECT ifNull(sum((x AS a1) IN [y] AS c3 = TRUE), 0)');

-- Variants exercising the same alias-wrap + nested-IN path.
SELECT formatQuerySingleLine('SELECT ifNull(sum((x AS a1) NOT IN [y] AS c3 = TRUE), 0)') =
       formatQuerySingleLine(formatQuerySingleLine('SELECT ifNull(sum((x AS a1) NOT IN [y] AS c3 = TRUE), 0)'));
SELECT formatQuerySingleLine('SELECT g((p IN (1, 2) AS q) = 1, (r IN [3] AS s) = 2)') =
       formatQuerySingleLine(formatQuerySingleLine('SELECT g((p IN (1, 2) AS q) = 1, (r IN [3] AS s) = 2)'));
SELECT formatQuerySingleLine('SELECT h(((a AS z) IN (SELECT 1) AS w) = 1, 0)') =
       formatQuerySingleLine(formatQuerySingleLine('SELECT h(((a AS z) IN (SELECT 1) AS w) = 1, 0)'));
SELECT formatQuerySingleLine('SELECT toInt32((x AS a) IN [1] AS b = 1)') =
       formatQuerySingleLine(formatQuerySingleLine('SELECT toInt32((x AS a) IN [1] AS b = 1)'));

-- Baselines that must keep round-tripping (guard against over-wrapping regressions).
SELECT formatQuerySingleLine('SELECT f(1, 2 IN ((3 IN (4, 5)) AS x))') =
       formatQuerySingleLine(formatQuerySingleLine('SELECT f(1, 2 IN ((3 IN (4, 5)) AS x))'));
SELECT formatQuerySingleLine('SELECT position((1 IN (SELECT 1)), 2)') =
       formatQuerySingleLine(formatQuerySingleLine('SELECT position((1 IN (SELECT 1)), 2)'));
SELECT formatQuerySingleLine('SELECT (1 IN (2) AS a) = (3 IN (4) AS b)') =
       formatQuerySingleLine(formatQuerySingleLine('SELECT (1 IN (2) AS a) = (3 IN (4) AS b)'));
