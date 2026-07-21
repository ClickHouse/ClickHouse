-- Null-safe comparison (IS DISTINCT FROM / IS NOT DISTINCT FROM) of values whose types
-- have no least common supertype (mixed signed/unsigned integers wider than 32 bits).

-- Scalars: mixed signed/unsigned integers
SELECT CAST('1', 'UInt64') IS DISTINCT FROM CAST('-1', 'Int64');
SELECT CAST('1', 'UInt64') IS NOT DISTINCT FROM CAST('-1', 'Int64');
SELECT CAST('1', 'UInt64') IS DISTINCT FROM CAST('1', 'Int64');
SELECT CAST('1', 'UInt64') IS NOT DISTINCT FROM CAST('1', 'Int64');

-- Wide integers
SELECT CAST('-1', 'Int256') IS DISTINCT FROM CAST('1', 'UInt256');
SELECT CAST('100', 'Int256') IS NOT DISTINCT FROM CAST('100', 'UInt256');

-- Non-constant (materialized)
SELECT materialize(CAST('1', 'UInt64')) IS DISTINCT FROM materialize(CAST('-1', 'Int64'));
SELECT materialize(CAST('1', 'UInt64')) IS NOT DISTINCT FROM materialize(CAST('1', 'Int64'));

-- Nullable scalars: NULLs are resolved null-safely, values via accurate comparison
SELECT CAST('1', 'Nullable(UInt64)') IS DISTINCT FROM CAST('-1', 'Nullable(Int64)');
SELECT CAST('1', 'Nullable(UInt64)') IS NOT DISTINCT FROM CAST('1', 'Nullable(Int64)');
SELECT NULL::Nullable(UInt64) IS DISTINCT FROM CAST('1', 'Nullable(Int64)');
SELECT NULL::Nullable(UInt64) IS NOT DISTINCT FROM CAST('1', 'Nullable(Int64)');
SELECT NULL::Nullable(UInt64) IS DISTINCT FROM NULL::Nullable(Int64);
SELECT NULL::Nullable(UInt64) IS NOT DISTINCT FROM NULL::Nullable(Int64);
-- One side Nullable, the other not
SELECT CAST('1', 'UInt64') IS DISTINCT FROM CAST('-1', 'Nullable(Int64)');
SELECT CAST('1', 'UInt64') IS DISTINCT FROM NULL::Nullable(Int64);

-- Multi-row via table (per-row NULLs and offsets)
DROP TABLE IF EXISTS t_null_safe_cmp;
CREATE TABLE t_null_safe_cmp (a Nullable(UInt64), b Nullable(Int64)) ENGINE = Memory;
INSERT INTO t_null_safe_cmp VALUES (1, 1), (1, -1), (NULL, 1), (1, NULL), (NULL, NULL);
SELECT a IS DISTINCT FROM b FROM t_null_safe_cmp ORDER BY a, b;
SELECT a IS NOT DISTINCT FROM b FROM t_null_safe_cmp ORDER BY a, b;
DROP TABLE t_null_safe_cmp;

-- Arrays whose element types have no least common supertype
SELECT [1]::Array(UInt64) IS DISTINCT FROM [-1]::Array(Int64);
SELECT [1]::Array(UInt64) IS NOT DISTINCT FROM [1]::Array(Int64);
SELECT [1,2]::Array(UInt64) IS DISTINCT FROM [1,2,3]::Array(Int64);
SELECT materialize([1]::Array(UInt64)) IS DISTINCT FROM materialize([-1]::Array(Int64));

-- Arrays of Nullable elements whose types have no least common supertype
SELECT [1]::Array(Nullable(UInt64)) IS DISTINCT FROM [-1]::Array(Nullable(Int64));
SELECT [1]::Array(Nullable(UInt64)) IS NOT DISTINCT FROM [1]::Array(Nullable(Int64));
SELECT [1,2]::Array(Nullable(UInt64)) IS NOT DISTINCT FROM [1,2]::Array(Nullable(Int64));
SELECT [NULL]::Array(Nullable(UInt64)) IS NOT DISTINCT FROM [NULL]::Array(Nullable(Int64));
SELECT [NULL]::Array(Nullable(UInt64)) IS DISTINCT FROM [1]::Array(Nullable(Int64));
SELECT materialize([1]::Array(Nullable(UInt64))) IS DISTINCT FROM materialize([-1]::Array(Nullable(Int64)));

-- Array/Map vs top-level NULL: a non-NULL Array/Map value and a NULL are always distinct
SELECT [1]::Array(UInt64) IS DISTINCT FROM NULL;
SELECT NULL IS DISTINCT FROM [1]::Array(UInt64);
SELECT [1]::Array(UInt64) IS NOT DISTINCT FROM NULL;
SELECT NULL IS NOT DISTINCT FROM [1]::Array(UInt64);
SELECT materialize([1]::Array(UInt64)) IS DISTINCT FROM NULL;
SELECT materialize([1]::Array(UInt64)) IS NOT DISTINCT FROM NULL;
SELECT map(1, 2)::Map(UInt64, UInt64) IS DISTINCT FROM NULL;
SELECT NULL IS NOT DISTINCT FROM map(1, 2)::Map(UInt64, UInt64);

-- Nullable wrapped string vs nullable number (no least common supertype, const-string path)
SELECT CAST('1', 'Nullable(String)') IS DISTINCT FROM CAST(1, 'Nullable(Int64)'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT CAST('1', 'Nullable(String)') IS NOT DISTINCT FROM CAST(1, 'Nullable(Int64)'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT CAST('2', 'Nullable(String)') IS DISTINCT FROM CAST(1, 'Nullable(Int64)'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT CAST('1', 'Nullable(FixedString(1))') IS NOT DISTINCT FROM CAST(1, 'Nullable(Int64)'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A top-level String/FixedString vs a type with no least common supertype is rejected
SELECT 'a' IS DISTINCT FROM 1::Int64; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT materialize('a') IS DISTINCT FROM materialize(1::Int64); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT 'a'::FixedString(1) IS NOT DISTINCT FROM 1::Int64; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT '2020-01-01' IS DISTINCT FROM toDate('2020-01-01'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(materialize('a') IS DISTINCT FROM materialize(1::Int64)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Consistency with the regular operators (null-safe result matches != / = for non-NULL values)
SELECT (CAST('1', 'UInt64') IS DISTINCT FROM CAST('-1', 'Int64')) = (CAST('1', 'UInt64') != CAST('-1', 'Int64'));
SELECT (CAST('1', 'UInt64') IS NOT DISTINCT FROM CAST('-1', 'Int64')) = (CAST('1', 'UInt64') = CAST('-1', 'Int64'));

-- Aligned String-vs-String subfield with a signed/unsigned mismatch in another subfield is
-- comparable null-safely and must match the regular != / = operators for these non-NULL values.
SELECT ([tuple('a', 1::UInt64)]::Array(Tuple(String, UInt64)) IS DISTINCT FROM     [tuple('a', -1::Int64)]::Array(Tuple(String, Int64))) = ([tuple('a', 1::UInt64)]::Array(Tuple(String, UInt64)) != [tuple('a', -1::Int64)]::Array(Tuple(String, Int64)));
SELECT ([tuple('a', 1::UInt64)]::Array(Tuple(String, UInt64)) IS NOT DISTINCT FROM [tuple('a', -1::Int64)]::Array(Tuple(String, Int64))) = ([tuple('a', 1::UInt64)]::Array(Tuple(String, UInt64)) =  [tuple('a', -1::Int64)]::Array(Tuple(String, Int64)));

-- A crossed String-vs-number subfield is still rejected null-safely
SELECT [tuple('a', 1::UInt64)]::Array(Tuple(String, UInt64)) IS DISTINCT FROM [tuple(1::Int64, 'a')]::Array(Tuple(Int64, String)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Top-level Nullable(Tuple(...)) with an aligned String subfield exercises the no-supertype
-- Nullable path (executeNullableWithoutSupertype) and matches the non-Nullable value comparison.
SET enable_nullable_tuple_type = 1;
SELECT (CAST(tuple('a', 1::UInt64), 'Nullable(Tuple(String, UInt64))') IS NOT DISTINCT FROM CAST(tuple('a', -1::Int64), 'Nullable(Tuple(String, Int64))')) = (tuple('a', 1::UInt64) = tuple('a', -1::Int64));
SELECT (CAST(tuple('a', 1::UInt64), 'Nullable(Tuple(String, UInt64))') IS DISTINCT FROM     CAST(tuple('a', -1::Int64), 'Nullable(Tuple(String, Int64))')) = (tuple('a', 1::UInt64) != tuple('a', -1::Int64));

-- Incomparable element types still throw
SELECT ['a']::Array(String) IS DISTINCT FROM [1]::Array(Int64); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
