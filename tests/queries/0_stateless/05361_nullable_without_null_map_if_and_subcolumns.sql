-- The JIT-compiled `if` keeps the null map, check the interpreted implementation.
SET compile_expressions = 0;

-- `if` over `Nullable` branches does not create a null map if the NULLs of the branches are not selected.
SELECT DISTINCT dumpColumnStructure(if(number < 100, toNullable(number), nullIf(number, 1))) FROM numbers(3);
SELECT DISTINCT dumpColumnStructure(if(number < 2, nullIf(number, 3), nullIf(number, 1))) FROM numbers(4);
-- The selected NULLs are kept.
SELECT DISTINCT dumpColumnStructure(if(number < 1, toNullable(number), nullIf(number, 1))) FROM numbers(3);
SELECT if(number < 2, toNullable(number), nullIf(number, 2)) AS x, isNull(x) FROM numbers(4);
SELECT if(number < 2, nullIf(number, 3), nullIf(number, 1)) AS x, isNull(x) FROM numbers(4);

-- The `null` subcolumn of a `Nullable` column without a null map.
SELECT toNullable(number) AS x, x.null FROM numbers(2);
SELECT CAST(tuple(number) AS Tuple(a Nullable(UInt64))) AS t, t.a, t.a.null FROM numbers(2);
SELECT arrayMap(x -> toNullable(x), [1, 2]) AS a, a.null;
