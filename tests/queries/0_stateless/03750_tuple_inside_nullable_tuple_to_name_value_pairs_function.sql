-- { echoOn }

SET allow_experimental_nullable_tuple_type = 1;

SELECT
    tupleToNameValuePairs(
        CAST((1, ('x', 7)) AS Nullable(Tuple(a Int32, b Tuple(String, Int32))))
    ) AS res,
    toTypeName(res); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT
    tupleToNameValuePairs(
        CAST(NULL AS Nullable(Tuple()))
    ) AS res,
    toTypeName(res); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT
    tupleToNameValuePairs(
        CAST(NULL AS Nullable(Tuple(a Nullable(Int32))))
    ) AS res,
    toTypeName(res);

SELECT
    tupleToNameValuePairs(
        CAST(NULL AS Nullable(Tuple(a Int32, b Int32)))
    ) AS res,
    toTypeName(res);

SELECT
    tupleToNameValuePairs(
        CAST(NULL AS Nullable(Tuple(Int32, Int32)))
    ) AS res,
    toTypeName(res);

SELECT
    tupleToNameValuePairs(
        CAST((NULL, tuple(1)) AS Nullable(Tuple(a Nullable(Tuple(Int32)), b Nullable(Tuple(Int32)))))
    ) AS res,
    toTypeName(res);

SELECT
    tupleToNameValuePairs(
        CAST(NULL AS Nullable(Tuple(Nullable(Tuple(Int32)), Nullable(Tuple(Int32)))))
    ) AS res,
    toTypeName(res);
