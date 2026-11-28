-- `ForeignKeyDeclaration` is not a node the SQL parser ever leaves in an AST (`FOREIGN KEY` clauses are
-- dropped by `ParserCreateQuery`) and it has no formatter, so the JSON AST reader must reject it with a
-- regular error instead of building an AST whose formatting fails with a logical error.
-- Found by json_ast_sql_parser_fuzzer.
SELECT formatQueryFromJSON('{"type":"ForeignKeyDeclaration"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"ForeignKeyDeclaration","name":"fk"}]}}]}}'); -- { serverError BAD_ARGUMENTS }

-- `FOREIGN KEY` itself still parses and round-trips (the clause is dropped, as by the SQL parser).
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`a` UInt8, FOREIGN KEY (a) REFERENCES t2 (b)) ENGINE = Memory'));
