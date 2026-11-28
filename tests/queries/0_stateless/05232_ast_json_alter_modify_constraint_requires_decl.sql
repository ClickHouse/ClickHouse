-- `ALTER TABLE ... MODIFY CONSTRAINT` without the constraint declaration passed the JSON AST reader and
-- `ASTAlterCommand::formatImpl` dereferenced the null `constraint_decl` (UBSan: member call on null pointer,
-- a segfault in release builds). Found by json_ast_sql_parser_fuzzer on the functional-test corpus.
SELECT formatQueryFromJSON('{"type":"AlterQuery","alter_object":"TABLE","table_ast":{"type":"Identifier","name":"t"},"command_list":{"type":"ExpressionList","children":[{"type":"AlterCommand","command_type":"MODIFY_CONSTRAINT","if_exists":true}]}}'); -- { serverError BAD_ARGUMENTS }

-- The parser-produced shape still round-trips.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY CONSTRAINT c CHECK a < 5'));
