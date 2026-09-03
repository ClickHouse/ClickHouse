-- `array_join_use_nulls` only applies to element types that can be placed inside `Nullable`,
-- exactly like `join_use_nulls`. For an element type that cannot, the column keeps its original
-- type and the empty-array rows keep getting the default value of that type.

SET array_join_use_nulls = 1;

-- Nullable element type: NULL padding, as documented.
SELECT 'nullable-able element';
SELECT x, toTypeName(x)
FROM (SELECT CAST([], 'Array(UInt8)') AS arr)
LEFT ARRAY JOIN arr AS x;

-- Element types that cannot be placed inside `Nullable`: default padding, original type.
SELECT 'Array element';
SELECT x, toTypeName(x)
FROM (SELECT CAST([], 'Array(Array(UInt8))') AS arr)
LEFT ARRAY JOIN arr AS x;

SELECT 'Map element';
SELECT x, toTypeName(x)
FROM (SELECT CAST([], 'Array(Map(String, UInt8))') AS arr)
LEFT ARRAY JOIN arr AS x;

-- `Tuple` can be placed inside `Nullable`, so it does get NULL padding.
SELECT 'Tuple element';
SELECT x, toTypeName(x)
FROM (SELECT CAST([], 'Array(Tuple(UInt8, String))') AS arr)
LEFT ARRAY JOIN arr AS x;

-- Non-empty arrays are unaffected in either case.
SELECT 'non-empty';
SELECT x, toTypeName(x)
FROM (SELECT [[1, 2], [3]] AS arr)
LEFT ARRAY JOIN arr AS x
ORDER BY x;
