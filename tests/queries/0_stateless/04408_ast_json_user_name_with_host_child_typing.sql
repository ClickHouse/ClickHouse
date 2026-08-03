-- Regression test for the AST JSON review hardening of `ASTUserNameWithHost`.
-- `ParserUserNameWithHost` produces the user name only as an identifier (optionally a query parameter)
-- or a string literal, and stores the host pattern as a string literal. `getStringFromAST` therefore
-- only handles those shapes and otherwise throws `LOGICAL_ERROR`. A `clickhouse_json` AST could carry
-- any node for `username`/`host_pattern` (e.g. a `SQLSecurity` definer whose user is a `Function`):
-- it would format fine, but `processSQLSecurityOption` calling `ASTUserNameWithHost::toString` would
-- reach the internal `LOGICAL_ERROR` branch. `readJSON` now validates these child shapes and rejects
-- the malformed payloads with `BAD_ARGUMENTS` at the JSON boundary instead.

-- An identifier user name round-trips unchanged (must NOT be rejected):
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE VIEW v DEFINER = u SQL SECURITY DEFINER AS SELECT 1'));

-- `username` as a non-literal, non-identifier node (a `Function`) is parser-impossible and rejected:
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE VIEW v DEFINER = u SQL SECURITY DEFINER AS SELECT 1'), '"username":{"type":"Identifier","name":"u"}', '"username":{"type":"Function","name":"now","no_empty_args":true}')); -- { serverError BAD_ARGUMENTS }

-- `username` as a non-string literal (a number) is parser-impossible and rejected:
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE VIEW v DEFINER = u SQL SECURITY DEFINER AS SELECT 1'), '"username":{"type":"Identifier","name":"u"}', '"username":{"type":"Literal","value":{"field_type":"UInt64","value":5}}')); -- { serverError BAD_ARGUMENTS }

-- `host_pattern` as a non-literal node (a `Function`) is parser-impossible and rejected:
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE VIEW v DEFINER = u SQL SECURITY DEFINER AS SELECT 1'), '"username":{"type":"Identifier","name":"u"}', '"username":{"type":"Identifier","name":"u"},"host_pattern":{"type":"Function","name":"now","no_empty_args":true}')); -- { serverError BAD_ARGUMENTS }

-- `host_pattern` as a non-string literal (a number) is parser-impossible and rejected:
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE VIEW v DEFINER = u SQL SECURITY DEFINER AS SELECT 1'), '"username":{"type":"Identifier","name":"u"}', '"username":{"type":"Identifier","name":"u"},"host_pattern":{"type":"Literal","value":{"field_type":"UInt64","value":5}}')); -- { serverError BAD_ARGUMENTS }
