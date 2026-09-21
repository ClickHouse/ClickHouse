-- `arrayIntersect` puts the `NULL` of the intersection into the result once, and it does so again in
-- every row that has one.

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
