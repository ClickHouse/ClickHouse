-- Parser-produced shapes of the parser-owned fields round-trip byte-identically.

-- `TEMPORARY` belongs to the table-target branch of `ParserDropQuery`.
SELECT formatQueryFromJSON(parseQueryToJSON('DROP TEMPORARY TABLE db.t'));
-- `PERMANENTLY` is parsed only for `DETACH`.
SELECT formatQueryFromJSON(parseQueryToJSON('DETACH TABLE db.t PERMANENTLY'));
-- The LIKE filter is parsed only for `TRUNCATE [ALL] TABLES FROM <db>`.
SELECT formatQueryFromJSON(parseQueryToJSON('TRUNCATE ALL TABLES FROM db NOT LIKE ''x%'''));
-- `DEDUPLICATE BY` column specifications: identifiers, asterisk with EXCEPT, COLUMNS matcher.
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE db.t DEDUPLICATE BY x, y'));
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t FINAL DEDUPLICATE BY * EXCEPT (a, b)'));
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t DEDUPLICATE BY COLUMNS(''.*id'')'));
-- `EXPLAIN` child-set ownership: `CURRENT TRANSACTION` explains nothing, `TABLE OVERRIDE` carries
-- a table function plus an override, every other kind carries the explained query.
SELECT formatQueryFromJSON(parseQueryToJSON('EXPLAIN CURRENT TRANSACTION'));
SELECT formatQueryFromJSON(parseQueryToJSON('EXPLAIN TABLE OVERRIDE mysql(''127.0.0.1:3306'', ''db'', ''t'', ''user'', ''pw'') PARTITION BY x'));

-- `is_temporary` on a database-only target would format as parser-impossible
-- `DROP TEMPORARY DATABASE` while `InterpreterDropQuery` still drops the database.
SELECT formatQueryFromJSON('{"type":"DropQuery","kind":"Drop","database":"db","is_temporary":true}'); -- { serverError BAD_ARGUMENTS }
-- `permanently` outside `DETACH` formats parser-impossible `... PERMANENTLY` SQL.
SELECT formatQueryFromJSON('{"type":"DropQuery","kind":"Drop","table":"t","permanently":true}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"DropQuery","kind":"Truncate","table":"t","permanently":true}'); -- { serverError BAD_ARGUMENTS }
-- The LIKE filter outside `TRUNCATE [ALL] TABLES FROM` formats parser-impossible SQL that
-- execution ignores (`InterpreterDropQuery` consults it only for the truncate-tables form).
SELECT formatQueryFromJSON('{"type":"DropQuery","kind":"Drop","database":"db","table":"t","like":"x%"}'); -- { serverError BAD_ARGUMENTS }
-- Orphaned modifier flags without a `like` pattern are parser-impossible.
SELECT formatQueryFromJSON('{"type":"DropQuery","kind":"Truncate","database":"db","has_tables":true,"not_like":true}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"DropQuery","kind":"Truncate","database":"db","has_tables":true,"case_insensitive_like":true}'); -- { serverError BAD_ARGUMENTS }

-- `deduplicate_by_columns` without `deduplicate` would format as parser-impossible
-- `OPTIMIZE TABLE t BY x` and smuggle a hidden column list past `ast.deduplicate`-based checks.
SELECT formatQueryFromJSON('{"type":"OptimizeQuery","table":{"type":"Identifier","name":"t"},"deduplicate_by_columns":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]}}'); -- { serverError BAD_ARGUMENTS }
-- An empty list is parser-impossible (`ParserList` behind `DEDUPLICATE BY` is non-empty).
SELECT formatQueryFromJSON('{"type":"OptimizeQuery","table":{"type":"Identifier","name":"t"},"deduplicate":true,"deduplicate_by_columns":{"type":"ExpressionList","children":[]}}'); -- { serverError BAD_ARGUMENTS }
-- Entries must be column specifications (identifier, asterisk, or COLUMNS matcher).
SELECT formatQueryFromJSON('{"type":"OptimizeQuery","table":{"type":"Identifier","name":"t"},"deduplicate":true,"deduplicate_by_columns":{"type":"ExpressionList","children":[{"type":"ExpressionList"}]}}'); -- { serverError BAD_ARGUMENTS }

-- `EXPLAIN CURRENT TRANSACTION` cannot carry an explained query.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('EXPLAIN AST SELECT 1'), '"kind":"EXPLAIN AST"', '"kind":"EXPLAIN CURRENT TRANSACTION"')); -- { serverError BAD_ARGUMENTS }
-- `EXPLAIN TABLE OVERRIDE` cannot carry an extra explained query.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('EXPLAIN TABLE OVERRIDE mysql(''127.0.0.1:3306'', ''db'', ''t'', ''user'', ''pw'') PARTITION BY x'), '"kind":"EXPLAIN TABLE OVERRIDE"', '"kind":"EXPLAIN TABLE OVERRIDE","query":{"type":"Identifier","name":"q"}')); -- { serverError BAD_ARGUMENTS }
-- ... and a leftover table function / override is rejected outside `EXPLAIN TABLE OVERRIDE`.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('EXPLAIN TABLE OVERRIDE mysql(''127.0.0.1:3306'', ''db'', ''t'', ''user'', ''pw'') PARTITION BY x'), '"kind":"EXPLAIN TABLE OVERRIDE"', '"kind":"EXPLAIN CURRENT TRANSACTION"')); -- { serverError BAD_ARGUMENTS }
