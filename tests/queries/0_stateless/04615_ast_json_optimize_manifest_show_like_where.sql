-- `OPTIMIZE TABLE ... MANIFEST`: the `manifest` flag selects a separate execution path in
-- `InterpreterOptimizeQuery`, so dropping it on an AST JSON round-trip silently turns a manifest
-- optimization into a regular `OPTIMIZE`.
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t MANIFEST'));
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE db.t FINAL DEDUPLICATE CLEANUP MANIFEST'));

-- `SHOW FULL TABLES`: `full` switches `InterpreterShowTablesQuery` from `SELECT name` to
-- `SELECT name, engine`, so it must be formatted, not just stored.
SELECT formatQueryFromJSON(parseQueryToJSON('SHOW FULL TABLES'));
SELECT formatQueryFromJSON(parseQueryToJSON('SHOW FULL TEMPORARY TABLES'));
SELECT formatQueryFromJSON(parseQueryToJSON('SHOW FULL TABLES FROM db NOT LIKE ''t%'' LIMIT 5'));
SELECT formatQueryFromJSON(parseQueryToJSON('SHOW FULL DICTIONARIES'));
SELECT formatQueryFromJSON(parseQueryToJSON('SHOW TABLES WHERE name = ''x'''));
SELECT formatQueryFromJSON(parseQueryToJSON('SHOW FULL COLUMNS FROM t ILIKE ''x%'''));
SELECT formatQueryFromJSON(parseQueryToJSON('SHOW COLUMNS FROM t WHERE field = ''a'''));

-- `ParserShowTablesQuery` accepts either a LIKE clause or a WHERE clause, never both, and the
-- interpreter ignores the WHERE whenever LIKE is set: reject the parser-impossible combination.
SELECT formatQueryFromJSON('{"type":"ShowTablesQuery","like":"t%","where_expression":{"type":"Identifier","name":"name"}}'); -- { serverError BAD_ARGUMENTS }

-- `NOT` / `ILIKE` flags cannot exist without a LIKE pattern.
SELECT formatQueryFromJSON('{"type":"ShowTablesQuery","not_like":true}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ShowTablesQuery","case_insensitive_like":true}'); -- { serverError BAD_ARGUMENTS }

-- The same contract for `SHOW COLUMNS`:
SELECT formatQueryFromJSON('{"type":"ShowColumnsQuery","table":"t","like":"x%","where_expression":{"type":"Identifier","name":"field"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ShowColumnsQuery","table":"t","not_like":true}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ShowColumnsQuery","table":"t","case_insensitive_like":true}'); -- { serverError BAD_ARGUMENTS }
