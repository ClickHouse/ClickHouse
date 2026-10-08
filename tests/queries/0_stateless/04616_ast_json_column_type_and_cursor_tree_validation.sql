-- `ParserColumnDeclaration` produces the `data_type` slot only through `ParserDataType`, whose
-- top-level node is always a data type. A payload with any other subtree there used to survive
-- `readJSON` and only fail later (parser-impossible DDL or `DataTypeFactory` errors).
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x UInt8) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x Enum8(''a'' = 1), y Tuple(UInt8, String)) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x DEFAULT 1) ENGINE = Memory'));

-- An `Identifier` (a valid JSON AST node of the wrong kind) in `data_type` must be rejected at the boundary.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (x UInt8) ENGINE = Memory'),
    '"data_type":{"type":"DataType","name":"UInt8"}',
    '"data_type":{"type":"Identifier","name":"UInt8"}')); -- { serverError BAD_ARGUMENTS }

-- `ParserStreamSettings` produces `cursor_tree` only as a flat `Map` of `(String path, unsigned
-- integer leaf)` tuples, and `buildCursorTree` relies on that shape via `safeGet`. Malformed
-- payloads must fail with `BAD_ARGUMENTS` at deserialization, not inside formatter/analyzer code.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t STREAM CURSOR {''a'': {''b'': 10}, ''c'': 20}'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t STREAM'));

-- A map element that is not a tuple.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('SELECT * FROM t STREAM CURSOR {''a'': 10}'),
    '{"field_type":"Tuple","value":[{"field_type":"String","value":"a"},{"field_type":"Int64","value":10}]}',
    '{"field_type":"String","value":"a"}')); -- { serverError BAD_ARGUMENTS }

-- A tuple of the wrong size.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('SELECT * FROM t STREAM CURSOR {''a'': 10}'),
    '"value":[{"field_type":"String","value":"a"},{"field_type":"Int64","value":10}]',
    '"value":[{"field_type":"String","value":"a"}]')); -- { serverError BAD_ARGUMENTS }

-- A leaf that is not an integer.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('SELECT * FROM t STREAM CURSOR {''a'': 10}'),
    '{"field_type":"Int64","value":10}',
    '{"field_type":"String","value":"x"}')); -- { serverError BAD_ARGUMENTS }

-- A path that is not a string.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('SELECT * FROM t STREAM CURSOR {''a'': 10}'),
    '{"field_type":"String","value":"a"}',
    '{"field_type":"Int64","value":1}')); -- { serverError BAD_ARGUMENTS }

-- A `cursor_tree` that is not a `Map` at all.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('SELECT * FROM t STREAM CURSOR {''a'': 10}'),
    '{"field_type":"Map",',
    '{"field_type":"Array",')); -- { serverError BAD_ARGUMENTS }
