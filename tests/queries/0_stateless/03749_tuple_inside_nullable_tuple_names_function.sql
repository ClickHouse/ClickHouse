-- { echoOn }

SET allow_experimental_nullable_tuple_type = 1;

SELECT
    tupleNames(
        CAST((1, ('x', 7)) AS Nullable(Tuple(a Int32, b Tuple(String, Int32))))
    ) AS res,
    toTypeName(res);

SELECT
    tupleNames(
        CAST(NULL AS Nullable(Tuple()))
    ) AS res,
    toTypeName(res);

SELECT
    tupleNames(
        CAST(NULL AS Nullable(Tuple(a Nullable(Int32))))
    ) AS res,
    toTypeName(res);

SELECT
    tupleNames(
        CAST(NULL AS Nullable(Tuple(a Int32, b Array(Int32))))
    ) AS res,
    toTypeName(res);

SELECT
    tupleNames(
        CAST(NULL AS Nullable(Tuple(Int32, Array(Int32))))
    ) AS res,
    toTypeName(res);

SELECT
    tupleNames(
        CAST((1, ('x', NULL)) AS Nullable(Tuple(a Int32, b Nullable(Tuple(String, Nullable(Tuple(Int32, Nullable(Int32))))))))
    ) AS res,
    toTypeName(res);
