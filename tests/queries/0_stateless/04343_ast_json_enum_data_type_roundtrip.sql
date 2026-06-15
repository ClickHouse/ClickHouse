-- Regression test: `ASTEnumDataType` stores its values directly in a `values` vector (not as AST
-- children), so the generic `ASTDataType` JSON serialization dropped them and `Enum8('a' = 1)`
-- round-tripped lossily as a bare `Enum8`. `ASTEnumDataType` now has its own `writeJSON`/`readJSON`
-- (under the `EnumDataType` type tag), so the enum values survive `formatQueryFromJSON(parseQueryToJSON(...))`.

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
