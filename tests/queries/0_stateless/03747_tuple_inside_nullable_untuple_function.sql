-- { echoOn }

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS test_empty;
CREATE TABLE test_empty (x Nullable(Tuple())) ENGINE = Memory;
INSERT INTO test_empty VALUES (()), (NULL);

SELECT untuple(x) FROM test_empty SETTINGS enable_analyzer = 1; -- { serverError EMPTY_LIST_OF_COLUMNS_QUERIED }

SELECT untuple(CAST(tuple() AS Nullable(Tuple()))) SETTINGS enable_analyzer = 1; -- { serverError EMPTY_LIST_OF_COLUMNS_QUERIED }

DROP TABLE IF EXISTS test_untuple_nullable_tuple;
CREATE TABLE test_untuple_nullable_tuple
(
    x Nullable(Tuple(Int32, String, Array(Int32)))
)
ENGINE = Memory;
INSERT INTO test_untuple_nullable_tuple VALUES ((1, 'a', [])), (NULL);

SELECT untuple(x)
FROM test_untuple_nullable_tuple
ORDER BY isNull(x), x;

SELECT untuple(
    CAST((1, 'a') AS Nullable(Tuple(Int32, String)))
);

SELECT untuple(
    CAST(NULL AS Nullable(Tuple(Int32, String)))
);

SELECT untuple(
    CAST(NULL AS Nullable(Tuple(Array(Int32), String)))
);
