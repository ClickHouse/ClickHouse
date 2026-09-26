SELECT arraySlice([1, 2, 3], 1);

SELECT number, arraySlice(arrayMap(x -> number + x, range(number)), 1)
FROM numbers(4);

SELECT arraySlice(arrayMap(x -> [x, x + 1], range(3)), 1);
SELECT arraySlice(['one', 'two', 'three'], 1);
SELECT arraySlice([toNullable(1), NULL, toNullable(3)], 1);
SELECT arraySlice(arrayMap(x -> (toString(x), x), range(3)), 1);

SELECT arraySlice([1, 2, 3], 1, NULL);

SELECT number, arraySlice(arrayMap(x -> number + x, range(3)), 1, NULL)
FROM numbers(3);

SELECT arraySlice([1, 2, 3], 2);
SELECT arraySlice([1, 2, 3], 1, 2);

SELECT number, x, arraySlice(big, 1)
FROM
(
    SELECT number, materialize(arrayMap(i -> concat('v', toString(i + number)), range(4))) AS big, range(3) AS r
    FROM numbers(2)
)
ARRAY JOIN r AS x
ORDER BY number, x
SETTINGS enable_lazy_columns_replication = 1;
