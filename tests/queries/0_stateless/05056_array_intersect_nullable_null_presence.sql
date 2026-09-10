-- A NULL belongs to the intersection only when it is present in every argument.

SELECT arrayIntersect(CAST([NULL, 2] AS Array(Nullable(UInt8))), CAST([1] AS Array(Nullable(UInt8))));
SELECT arrayIntersect(CAST([1] AS Array(Nullable(UInt8))), CAST([NULL, 2] AS Array(Nullable(UInt8))));
SELECT arrayIntersect([1, NULL], [1], [1, NULL]);
SELECT arrayIntersect(
    materialize(CAST([NULL] AS Array(Nullable(UInt8)))),
    materialize(CAST([1] AS Array(Nullable(UInt8)))))
FROM numbers(2);
SELECT arraySort(arrayIntersect(CAST([NULL, 2] AS Array(Nullable(UInt8))), CAST([1, NULL, 2] AS Array(Nullable(UInt8)))));
