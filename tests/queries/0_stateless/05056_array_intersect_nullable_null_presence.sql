-- A NULL belongs to the intersection only when it is present in every argument.

SELECT arrayIntersect(CAST([NULL, 2] AS Array(Nullable(UInt8))), CAST([1] AS Array(Nullable(UInt8))));
SELECT arrayIntersect(CAST([1] AS Array(Nullable(UInt8))), CAST([NULL, 2] AS Array(Nullable(UInt8))));
SELECT arrayIntersect([1, NULL], [1], [1, NULL]);
SELECT arrayIntersect(
    materialize(CAST([NULL] AS Array(Nullable(UInt8)))),
    materialize(CAST([1] AS Array(Nullable(UInt8)))))
FROM numbers(2);
SELECT arraySort(arrayIntersect(CAST([NULL, 2] AS Array(Nullable(UInt8))), CAST([1, NULL, 2] AS Array(Nullable(UInt8)))));

-- The `NULL` of the intersection goes into the result once, and again in every row that
-- has one: the count of `NULL` arguments and the flag for an already emitted `NULL` are
-- both per-row state.
SELECT arrayIntersect(CAST([NULL, 1, NULL] AS Array(Nullable(UInt8))), CAST([1, NULL] AS Array(Nullable(UInt8))));
SELECT arrayIntersect(CAST([NULL, 1, NULL] AS Array(Nullable(UInt8))));

SELECT id, arrayIntersect(a, b)
FROM values('id UInt8, a Array(Nullable(UInt8)), b Array(Nullable(UInt8))',
    (1, [NULL, 1, NULL], [1, NULL]), (2, [NULL, 2], [NULL, 2]), (3, [NULL, 3], [3]))
ORDER BY id;

-- The `NULL` comes from a constant argument.
SELECT id, arrayIntersect(CAST([NULL, 1] AS Array(Nullable(UInt8))), b)
FROM values('id UInt8, b Array(Nullable(UInt8))', (1, [NULL, 2]), (2, [1, 2]), (3, [NULL, 1]))
ORDER BY id;
