-- Shapes that the SQL parser never produces but the JSON AST reader accepted, so formatting hit a
-- logical error or an assertion. Found by json_ast_sql_parser_fuzzer.

-- `UNKNOWN` is not a SYSTEM command: `formatImpl` throws a logical error for it.
SELECT formatQueryFromJSON('{"type":"SystemQuery","query_type":"UNKNOWN"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SystemQuery","query_type":"END"}'); -- { serverError BAD_ARGUMENTS }

-- Commands with a mandatory table name assert on it while formatting.
SELECT formatQueryFromJSON('{"type":"SystemQuery","query_type":"REFRESH_VIEW"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SystemQuery","query_type":"START"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SystemQuery","query_type":"FLUSH_OBJECT_STORAGE_QUEUE","queue_path":"p"}'); -- { serverError BAD_ARGUMENTS }

-- The node type must be a JSON string; `null` used to escape as a bare Poco exception.
SELECT formatQueryFromJSON('{"type":null}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"AlterQuery","alter_object":"TABLE","table_ast":{"type":"Identifier","name":"t"},"command_list":{"type":null}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":123}'); -- { serverError BAD_ARGUMENTS }

-- The parser-produced shapes still round-trip.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM REFRESH VIEW db.mv'));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM STOP MERGES'));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM START MERGES t'));
