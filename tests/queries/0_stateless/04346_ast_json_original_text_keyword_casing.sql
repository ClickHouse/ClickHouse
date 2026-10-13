-- Regression tests for the two-argument `formatQueryFromJSON(json, original_query)` form, which
-- re-applies the original query's formatting (whitespace, comments, and keyword casing) on top of the
-- canonical AST. Only genuine SQL keywords are case-insensitive: an identifier whose name collides
-- with a keyword (e.g. a column named `Date`) must keep its AST casing, not be lowercased to the
-- original text, otherwise a case-sensitive identifier would silently change.

-- A keyword-named identifier (`Date`) keeps its AST casing even when the original text lowercases it,
-- while the surrounding genuine keywords (`select`/`from`) follow the original casing.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT Date FROM t'), 'select date from t');

-- An ALL-UPPERCASE keyword-shaped identifier (a column named `DATE`, which the canonical formatter
-- prints bare as `DATE`, indistinguishable by spelling from the `DATE` keyword) must also keep its AST
-- casing. Recasing it to the original `date` would not round-trip to the same AST, so the formatter
-- falls back to the canonical form (genuine keywords uppercased, the identifier preserved) instead of
-- emitting `date`.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT DATE FROM t'), 'select date from t');

-- Plain identifier casing is taken from the AST, not the original text.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t'), 'select A from t');

-- Genuine keywords are case-insensitive and follow the original text's casing.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1'), 'select 1');
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1'), 'SeLeCt 1');

-- Original whitespace and comments are preserved where the tokens match.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 + 2'), 'SELECT 1   +   2 /* c */');

-- The optional `original_query` is bounded by `max_query_size`: an oversized second argument skips the
-- whitespace-preservation pass (which would tokenize all of it) and falls back to canonical formatting.
-- Here the JSON fits the limit but the 200000-byte original does not, so the result is the canonical form.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 + 2'), repeat('x', 200000)) SETTINGS max_query_size = 100000;
