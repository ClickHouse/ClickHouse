-- Regression test for the specialized `ASTDataType` subclasses that store data outside the generic
-- `arguments` child: `ASTEnumDataType` (its `'name' = value` pairs in `values`) and `ASTTupleDataType`
-- (named-tuple field names in `element_names`). They inherited `ASTDataType::writeJSON`, which serializes
-- only `name`/`arguments`, so `Enum8('a' = 1)` round-tripped as a bare `Enum8` and `Tuple(a UInt8)` lost
-- its field name. Both now have dedicated `writeJSON`/`readJSON` (under the `EnumDataType`/`TupleDataType`
-- type tags) so the values/names survive `formatQueryFromJSON(parseQueryToJSON(...))`.

-- Explicit Enum8 / Enum16 values are preserved (this is the case that used to be lossy).
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`e` Enum8(\'a\' = 1, \'b\' = 2)) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`e` Enum16(\'a\' = 100, \'b\' = 200)) ENGINE = Memory'));

-- Negative and zero enum values round-trip.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`e` Enum8(\'a\' = -1, \'b\' = 0, \'c\' = 1)) ENGINE = Memory'));

-- A single-value enum.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`e` Enum8(\'only\' = 42)) ENGINE = Memory'));

-- Enum nested inside another type still preserves its values.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`e` Nullable(Enum8(\'a\' = 1, \'b\' = 2))) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`e` Array(Enum8(\'a\' = 1))) ENGINE = Memory'));

-- Auto-assigned enum (`Enum8('a', 'b')`) takes the generic `ASTDataType` path and also round-trips.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`e` Enum8(\'a\', \'b\')) ENGINE = Memory'));

-- Named tuples preserve their field names (the case that used to drop names), while unnamed tuples
-- and nested named tuples also round-trip.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`x` Tuple(a UInt8, b String)) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`x` Tuple(UInt8, String)) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`x` Array(Tuple(a UInt8, b String))) ENGINE = Memory'));
