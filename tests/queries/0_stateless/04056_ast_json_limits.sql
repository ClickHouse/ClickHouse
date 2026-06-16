-- Test that parseQueryToJSON respects parser limits and
-- formatQueryFromJSON enforces AST size/depth limits.

-- parseQueryToJSON should respect max_parser_depth.
-- Use a value high enough for the outer query to parse, but low enough
-- for the deeply-nested inner query to fail.
SET max_parser_depth = 50;
SELECT parseQueryToJSON('SELECT ((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((1))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))'); -- { serverError TOO_DEEP_RECURSION }

-- Reset to default.
SET max_parser_depth = 1000;

-- formatQueryFromJSON should enforce max_ast_depth.
-- Use a limit high enough for the outer query (depth ~8) but low enough
-- for the reconstructed AST of 1+2+3+4+5 (depth ~12) to exceed it.
SET max_ast_depth = 10;
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 + 2 + 3 + 4 + 5')); -- { serverError TOO_DEEP_AST }

-- Reset to default.
SET max_ast_depth = 1000;

-- formatQueryFromJSON should enforce max_ast_elements.
-- The outer query has ~8 AST elements, so use a limit above that
-- but below the count of the reconstructed AST (which has many columns and table references).
SET max_ast_elements = 15;
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a, b, c, d, e, f, g, h, i, j, k, l FROM t')); -- { serverError TOO_BIG_AST }

-- Reset to default.
SET max_ast_elements = 50000;

-- `max_ast_elements` must also bound non-AST arrays that live outside the AST-node tree
-- (enum `values`, BACKUP/RESTORE elements and their `EXCEPT` sets, string arrays), otherwise a
-- tiny-AST payload could carry millions of such entries and allocate them before any limit fires.
-- The baseline query (small fixed structure) fits the limit; adding many non-AST entries exceeds it.
SET max_ast_elements = 12;
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (e Enum8(\'a\' = 1)) ENGINE = Memory')) FORMAT Null;
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (e Enum8(\'a\'=1, \'b\'=2, \'c\'=3, \'d\'=4, \'e\'=5, \'f\'=6, \'g\'=7, \'h\'=8, \'i\'=9, \'j\'=10)) ENGINE = Memory')); -- { serverError TOO_BIG_AST }
SELECT formatQueryFromJSON(parseQueryToJSON('RESTORE ALL FROM Disk(\'disk\', \'/b/\')')) FORMAT Null;
SELECT formatQueryFromJSON(parseQueryToJSON('RESTORE ALL EXCEPT DATABASES d1, d2, d3, d4, d5, d6, d7, d8, d9, d10 FROM Disk(\'disk\', \'/b/\')')); -- { serverError TOO_BIG_AST }
SELECT formatQueryFromJSON(parseQueryToJSON('RESTORE DATABASE db EXCEPT TABLES db.t1, db.t2, db.t3, db.t4, db.t5, db.t6, db.t7, db.t8, db.t9, db.t10 FROM Disk(\'disk\', \'/b/\')')); -- { serverError TOO_BIG_AST }

-- Reset to default.
SET max_ast_elements = 50000;

-- Malformed JSON should produce a clear error.
SELECT formatQueryFromJSON('not json'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"NoSuchASTNode"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExpressionList","children":[null]}'); -- { serverError BAD_ARGUMENTS }

-- ASTOrderByElement: only direction values -1 and 1 are valid; reject others.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT a FROM t ORDER BY a ASC'), '"direction":1', '"direction":0')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT a FROM t ORDER BY a ASC'), '"nulls_direction":1', '"nulls_direction":2')); -- { serverError BAD_ARGUMENTS }

-- Field value must be a JSON object: malformed shape (e.g. string instead of object) must be rejected
-- instead of silently becoming NULL.
SELECT formatQueryFromJSON('{"type":"Literal","value":"oops"}'); -- { serverError BAD_ARGUMENTS }

-- A `value` payload routed through `Field::restoreFromDump` (i.e. a `field_type` that is not one of
-- the explicitly-handled scalar/Array/Tuple/Map cases) must respect `max_ast_depth`. Otherwise a
-- shallow JSON object could smuggle a deeply nested `Array_[Array_[...]]` dump and exhaust the stack.
SET max_ast_depth = 8;
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"AggregateFunctionState","value":"Array_[Array_[Array_[Array_[Array_[Array_[Array_[Array_[Array_[Array_[Array_[Array_[1]]]]]]]]]]]]"}}'); -- { serverError BAD_ARGUMENTS }
SET max_ast_depth = 1000;

-- ASTOrderByElement: `expression` is required; otherwise `formatImpl` dereferences `children.front()`.
SELECT formatQueryFromJSON('{"type":"OrderByElement","direction":1,"nulls_direction":1}'); -- { serverError BAD_ARGUMENTS }

-- ASTTTLElement: unknown `mode` / `destination_type` must be rejected, and `ttl_expr` is required.
SELECT formatQueryFromJSON('{"type":"TTLElement","mode":"NoSuchMode","destination_type":"DELETE","if_exists":false}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"TTLElement","mode":"DELETE","destination_type":"NoSuchDestination","if_exists":false}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"TTLElement","mode":"DELETE","destination_type":"DELETE","if_exists":false}'); -- { serverError BAD_ARGUMENTS }

-- ASTRefreshStrategy: `period` is required for `schedule_kind` AFTER / EVERY.
SELECT formatQueryFromJSON('{"type":"RefreshStrategy","schedule_kind":"AFTER"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"RefreshStrategy","schedule_kind":"EVERY"}'); -- { serverError BAD_ARGUMENTS }
