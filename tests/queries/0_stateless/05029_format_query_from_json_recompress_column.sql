-- `ASTAlterCommand::readJSON` must reject a `RECOMPRESS_COLUMN` payload without `column`:
-- `formatImpl` and `MutationCommand::parse` dereference it unconditionally, so a malformed
-- serialized AST must fail with `BAD_ARGUMENTS` at deserialization instead of later.
SELECT formatQueryFromJSON('{"type":"AlterQuery","alter_object":"TABLE","table":"t","command_list":{"type":"ExpressionList","children":[{"type":"AlterCommand","command_type":"RECOMPRESS_COLUMN"}]}}'); -- { serverError BAD_ARGUMENTS }

-- The well-formed payload still round-trips.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t RECOMPRESS COLUMN c'));
