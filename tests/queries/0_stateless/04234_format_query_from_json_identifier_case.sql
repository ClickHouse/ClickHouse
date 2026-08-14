-- Regression: the two-argument `formatQueryFromJSON(json, original)` form must
-- preserve only keyword casing from `original`, not identifier casing.
-- Identifiers are case-sensitive in ClickHouse, so an AST that names a column
-- `Foo` must not be rewritten to `foo` just because `original` happened to write
-- the same letters in a different case.

-- Keywords (SELECT, FROM, WHERE) ARE case-insensitive — the original's
-- lowercase keyword text should be preserved.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t'), 'select a from t');

-- Identifiers differ in case between AST (`Foo`) and original (`foo`).
-- Output must use the AST identifier `Foo`, not the original `foo`.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT Foo FROM t'), 'SELECT foo FROM t');

-- Same in reverse: AST says `foo`, original says `Foo`. Output must be `foo`.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT foo FROM t'), 'SELECT Foo FROM t');

-- Mixed: keyword case preserved from original, identifier case from AST.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT MyCol FROM MyTable'), 'select mycol from mytable');

-- Identical input: should round-trip exactly.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t'), 'SELECT a FROM t');
