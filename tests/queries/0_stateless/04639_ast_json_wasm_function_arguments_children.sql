-- `ParserCreateFunctionQuery` builds `ARGUMENTS (...)` only from `ParserNameTypePairList` or
-- `ParserTypeList` (or leaves the list empty), so every child of the `arguments` expression list is
-- either an `ASTNameTypePair` or a data-type node, and the two shapes never mix within one list.
-- `validateAndGetDefinition` forwards every non-`ASTNameTypePair` child to `DataTypeFactory::get`,
-- so a foreign child injected through `clickhouse_json` would either format as parser-impossible
-- SQL (`ARGUMENTS (1)`) or fail later as an internal AST-structure error. Reject it at the
-- deserialization boundary instead.

-- Parser-produced shapes round-trip byte-identically.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE FUNCTION wasm_f LANGUAGE WASM ABI ROW_DIRECT FROM ''module1'' ARGUMENTS (num UInt32, str String) RETURNS UInt32'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE FUNCTION wasm_f LANGUAGE WASM FROM ''module1'' ARGUMENTS (UInt32, String) RETURNS UInt32'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE FUNCTION wasm_f LANGUAGE WASM FROM ''module1'' ARGUMENTS () RETURNS UInt32'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE FUNCTION wasm_f LANGUAGE WASM FROM ''module1'' ARGUMENTS (nested Tuple(a UInt32, b String)) RETURNS UInt32'));

-- A `Literal` in the `arguments` list is parser-impossible and would format as `ARGUMENTS (1)`.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE FUNCTION wasm_f LANGUAGE WASM FROM ''module1'' ARGUMENTS (UInt32) RETURNS String'),
    '{"type":"DataType","name":"UInt32"}',
    '{"type":"Literal","value":{"field_type":"UInt64","value":1}}')); -- { serverError BAD_ARGUMENTS }

-- An arbitrary `Function` node would reach `DataTypeFactory::get` as an unexpected AST structure.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE FUNCTION wasm_f LANGUAGE WASM FROM ''module1'' ARGUMENTS (UInt32) RETURNS String'),
    '{"type":"DataType","name":"UInt32"}',
    '{"type":"Function","name":"foo","no_empty_args":true}')); -- { serverError BAD_ARGUMENTS }

-- Mixing name-type pairs and bare data types within one list is parser-impossible
-- (`ParserNameTypePairList` and `ParserTypeList` each own the whole list).
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE FUNCTION wasm_f LANGUAGE WASM FROM ''module1'' ARGUMENTS (num UInt32, str String) RETURNS UInt32'),
    '{"type":"NameTypePair","name":"str","name_type":{"type":"DataType","name":"String"}}',
    '{"type":"DataType","name":"String"}')); -- { serverError BAD_ARGUMENTS }
