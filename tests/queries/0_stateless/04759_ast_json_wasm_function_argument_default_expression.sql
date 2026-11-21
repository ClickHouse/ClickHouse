-- `ParserCreateFunctionQuery` builds `ARGUMENTS (...)` with `ParserNameTypePairList`, which never
-- attaches an element `DEFAULT` expression: that shape exists only for `Tuple`/`Nested` elements
-- and is normalized away by `pullUpTupleElementDefaults`. A `default_expression` injected into a
-- WASM function argument through `clickhouse_json` would format as parser-impossible SQL
-- (`ARGUMENTS (num UInt32 DEFAULT 1)`) while `validateAndGetDefinition` silently ignores it, so it
-- must be rejected at the deserialization boundary.

-- A parser-produced name-type pair round-trips byte-identically.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE FUNCTION wasm_f LANGUAGE WASM FROM ''module1'' ARGUMENTS (num UInt32) RETURNS UInt32'));

-- The same pair with an injected `default_expression` fails closed.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE FUNCTION wasm_f LANGUAGE WASM FROM ''module1'' ARGUMENTS (num UInt32) RETURNS UInt32'),
    '{"type":"NameTypePair","name":"num","name_type":{"type":"DataType","name":"UInt32"}}',
    '{"type":"NameTypePair","name":"num","name_type":{"type":"DataType","name":"UInt32"},"default_expression":{"type":"Literal","value":{"field_type":"UInt64","value":1}}}')); -- { serverError BAD_ARGUMENTS }
