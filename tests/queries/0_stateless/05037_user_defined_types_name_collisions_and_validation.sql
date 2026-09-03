-- Tags: no-parallel
-- Tag no-parallel: user-defined types live in a single process-wide namespace.

DROP TYPE IF EXISTS Maybe;
DROP TYPE IF EXISTS Broken;
DROP TYPE IF EXISTS ShownType;

-- `DataTypeFactory` resolves user-defined types before the built-in creators, so a user-defined
-- type named after a built-in type, one of its aliases or a type family would hijack every later
-- use of that name.
CREATE TYPE UInt64 AS String; -- { serverError BAD_ARGUMENTS }
CREATE TYPE Array AS String; -- { serverError BAD_ARGUMENTS }
CREATE TYPE INT AS String; -- { serverError BAD_ARGUMENTS }
SELECT toTypeName(CAST(1, 'UInt64')), toTypeName(CAST([1], 'Array(UInt64)'));

-- The base type is validated against the registered type families, not against a hard-coded list,
-- so a registered family that was missing from that list is accepted.
CREATE TYPE Maybe(T) AS Variant(T, String);
SHOW TYPE Maybe;
SELECT toTypeName(CAST(1::UInt64, 'Maybe(UInt64)'));
DROP TYPE Maybe;

-- A definition naming a known family but with a wrong number of arguments is rejected at
-- `CREATE TYPE` time instead of only failing later, when the type is used.
CREATE TYPE Broken AS Map(String); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SHOW TYPE Broken; -- { serverError UNKNOWN_TYPE }

-- `SHOW TYPES` and `SHOW TYPE` go through `ParserQueryWithOutput`, so they support the usual
-- output clauses of the other `SHOW` queries.
CREATE TYPE ShownType AS UInt64;
SHOW TYPE ShownType FORMAT JSONEachRow;
SHOW TYPE ShownType SETTINGS max_result_rows = 10;
SELECT formatQuerySingleLine('SHOW TYPES FORMAT JSONEachRow');
SELECT formatQuerySingleLine('SHOW TYPES SETTINGS max_result_rows = 10');
DROP TYPE ShownType;
