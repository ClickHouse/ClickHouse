-- `AUTO_INCREMENT` is part of the parser contract even without a `default_expression`, and it changes
-- behavior on execution (rejected under `compatibility_ignore_auto_increment_in_create_table = 0`),
-- so formatting must not drop it. When the type is omitted, the parser synthesizes `INT`.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (id Int32 AUTO_INCREMENT) ENGINE = MergeTree ORDER BY tuple()'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (id AUTO_INCREMENT, x UInt8) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (id Int32 AUTO_INCREMENT) ENGINE = MergeTree ORDER BY tuple()'))
     = formatQuerySingleLine('CREATE TABLE t (id Int32 AUTO_INCREMENT) ENGINE = MergeTree ORDER BY tuple()');

-- The keyword still matters after the round-trip: executing a `CREATE` with `AUTO_INCREMENT` is rejected
-- under the default `compatibility_ignore_auto_increment_in_create_table = 0`.
CREATE TABLE t04617 (id Int32 AUTO_INCREMENT) ENGINE = Memory; -- { serverError SYNTAX_ERROR }

-- The parser never combines `AUTO_INCREMENT` with an expression; such a pair must be rejected.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (id Int32 DEFAULT 1) ENGINE = Memory'),
    '"default_specifier":"DEFAULT"',
    '"default_specifier":"AUTO_INCREMENT"')); -- { serverError BAD_ARGUMENTS }

-- `CreateWasmFunctionQuery.result_type` is parser-produced only through `ParserDataType`; any other
-- subtree must be rejected at the JSON boundary instead of surviving until later validation.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE FUNCTION wasm_f LANGUAGE WASM ABI ROW_DIRECT FROM ''module1'' ARGUMENTS (num UInt32) RETURNS UInt32'));
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE FUNCTION wasm_f LANGUAGE WASM ABI ROW_DIRECT FROM ''module1'' ARGUMENTS (num UInt32) RETURNS UInt32'),
    '"result_type":{"type":"DataType","name":"UInt32"}',
    '"result_type":{"type":"Identifier","name":"UInt32"}')); -- { serverError BAD_ARGUMENTS }

-- `Columns.primary_key`/`primary_key_from_columns` are parser-intermediate slots that `ParserCreateQuery`
-- always normalizes into `storage.primary_key`; accepting them from JSON would carry a hidden primary-key
-- request that both formatting and execution silently drop.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x UInt8, PRIMARY KEY (x)) ENGINE = MergeTree'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x UInt8) ENGINE = MergeTree PRIMARY KEY x'));
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (x UInt8) ENGINE = MergeTree PRIMARY KEY x'),
    '"type":"Columns definition","columns":',
    '"type":"Columns definition","primary_key":{"type":"Identifier","name":"x"},"columns":')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (x UInt8) ENGINE = MergeTree PRIMARY KEY x'),
    '"type":"Columns definition","columns":',
    '"type":"Columns definition","primary_key_from_columns":{"type":"Identifier","name":"x"},"columns":')); -- { serverError BAD_ARGUMENTS }
