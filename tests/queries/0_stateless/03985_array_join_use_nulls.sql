-- Test array_join_use_nulls setting

-- Default behavior (array_join_use_nulls=0): empty arrays produce default values
SELECT 'default behavior';
SELECT id, x FROM (SELECT 1 AS id, [10, 20] AS arr UNION ALL SELECT 2 AS id, [] AS arr) LEFT ARRAY JOIN arr AS x ORDER BY id, x;

-- With array_join_use_nulls=1: empty arrays produce NULL
SET array_join_use_nulls = 1;
SELECT 'array_join_use_nulls=1';
SELECT id, x FROM (SELECT 1 AS id, [10, 20] AS arr UNION ALL SELECT 2 AS id, [] AS arr) LEFT ARRAY JOIN arr AS x ORDER BY id, x;

-- Verify NULL is produced (not default value)
SELECT 'null check';
SELECT id, x IS NULL FROM (SELECT 1 AS id, [10, 20] AS arr UNION ALL SELECT 2 AS id, [] AS arr) LEFT ARRAY JOIN arr AS x ORDER BY id, x;

-- Already-nullable element types: no double wrapping
SELECT 'already nullable';
SELECT id, x FROM (SELECT 1 AS id, [toNullable(10), toNullable(20)] AS arr UNION ALL SELECT 2 AS id, [] :: Array(Nullable(UInt32)) AS arr) LEFT ARRAY JOIN arr AS x ORDER BY id, x;

-- String type
SELECT 'string type';
SELECT id, x FROM (SELECT 1 AS id, ['a', 'b'] AS arr UNION ALL SELECT 2 AS id, [] :: Array(String) AS arr) LEFT ARRAY JOIN arr AS x ORDER BY id, x;

-- Regular ARRAY JOIN is unaffected by the setting (drops empty arrays)
SELECT 'regular array join';
SELECT id, x FROM (SELECT 1 AS id, [10, 20] AS arr UNION ALL SELECT 2 AS id, [] AS arr) ARRAY JOIN arr AS x ORDER BY id, x;

-- Unaligned array join with array_join_use_nulls: padded elements should be NULL
SET enable_unaligned_array_join = 1;
SELECT 'unaligned left array join';
SELECT id, x, y FROM (SELECT 1 AS id, [10, 20] AS a1, [100] AS a2 UNION ALL SELECT 2 AS id, [] AS a1, [] AS a2) LEFT ARRAY JOIN a1 AS x, a2 AS y ORDER BY id, x, y;
SET enable_unaligned_array_join = 0;

-- With array_join_use_nulls=0 (explicit reset): unchanged behavior
SET array_join_use_nulls = 0;
SELECT 'array_join_use_nulls=0';
SELECT id, x FROM (SELECT 1 AS id, [10, 20] AS arr UNION ALL SELECT 2 AS id, [] AS arr) LEFT ARRAY JOIN arr AS x ORDER BY id, x;
