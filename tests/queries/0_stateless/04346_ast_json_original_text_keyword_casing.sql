-- Regression tests for the two-argument `formatQueryFromJSON(json, original_query)` form, which
-- re-applies the original query's formatting (whitespace, comments, and keyword casing) on top of the
-- canonical AST. Only genuine SQL keywords are case-insensitive: an identifier whose name collides
-- with a keyword (e.g. a column named `Date`) must keep its AST casing, not be lowercased to the
-- original text, otherwise a case-sensitive identifier would silently change.

-- A keyword-named identifier (`Date`) keeps its AST casing even when the original text lowercases it,
-- while the surrounding genuine keywords (`select`/`from`) follow the original casing.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT Date FROM t'), 'select date from t');

-- Plain identifier casing is taken from the AST, not the original text.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t'), 'select A from t');

-- Genuine keywords are case-insensitive and follow the original text's casing.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1'), 'select 1');
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1'), 'SeLeCt 1');

-- Original whitespace and comments are preserved where the tokens match.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 + 2'), 'SELECT 1   +   2 /* c */');
