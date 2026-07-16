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

-- Consistency with the regular operators (null-safe result matches != / = for non-NULL values)
SELECT (CAST('1', 'UInt64') IS DISTINCT FROM CAST('-1', 'Int64')) = (CAST('1', 'UInt64') != CAST('-1', 'Int64'));
SELECT (CAST('1', 'UInt64') IS NOT DISTINCT FROM CAST('-1', 'Int64')) = (CAST('1', 'UInt64') = CAST('-1', 'Int64'));

-- Incomparable element types still throw
SELECT ['a']::Array(String) IS DISTINCT FROM [1]::Array(Int64); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
