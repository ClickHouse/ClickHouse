-- A `NULL` inside a literal collection does not disqualify it from being read with the target type,
-- as long as the target wraps the numbers in a `Nullable` - which is what reads the `NULL` back.
-- https://github.com/ClickHouse/ClickHouse/issues/116025

SET session_timezone = 'UTC';

-- All the ways of writing the cast agree, and keep every digit.
SELECT CAST([0.1, NULL] AS Array(Nullable(Decimal256(76))));
SELECT [0.1, NULL]::Array(Nullable(Decimal256(76)));
SELECT CAST('[0.1, NULL]' AS Array(Nullable(Decimal256(76))));
SELECT CAST([0.1, NULL] AS Array(Nullable(Decimal256(76)))) = CAST('[0.1, NULL]' AS Array(Nullable(Decimal256(76))));

SELECT CAST([607668569663131286404589520, NULL] AS Array(Nullable(UInt128)));
SELECT CAST([-607668569663131286404589520, NULL] AS Array(Nullable(Int128)));
SELECT CAST([NULL] AS Array(Nullable(Int256)));
SELECT CAST([[0.1, NULL], [NULL]] AS Array(Array(Nullable(Decimal256(76)))));

-- The keyword is read in any case.
SELECT CAST([0.1, null, NuLL] AS Array(Nullable(Decimal256(76))));

-- The types that convert the number keep converting it.
SELECT CAST([1, NULL] AS Array(Nullable(DateTime)));
SELECT CAST([0.1, NULL] AS Array(Nullable(Float64)));

-- A `NULL` with a target that cannot hold it keeps the old conversion error, instead of the text
-- parser silently turning it into a default value.
SELECT CAST([1, NULL] AS Array(UInt8)); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT CAST([1, NULL] AS Array(UInt128)); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT [1, NULL]::Array(UInt8); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT (1, NULL)::Tuple(UInt8, UInt8); -- { serverError CANNOT_CONVERT_TYPE }

-- A tuple with a `NULL` and collections of strings still work through the expression path.
SELECT (1, NULL)::Tuple(UInt8, Nullable(UInt8));
SELECT [NULL, 'a']::Array(Nullable(String));
