-- { echo }

SET allow_experimental_nullable_tuple_type = 0;

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    json Nullable(JSON(
        a UInt32,
        b Array(UInt32),
        c Nullable(UInt32),
        d Tuple(e UInt32, f Nullable(UInt32))
    ))
) ENGINE = Memory;

INSERT INTO test
SELECT NULL
FROM numbers(4);

SELECT json.d AS path, toTypeName(path) FROM test;

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    json Nullable(JSON(
        a UInt32,
        b Array(UInt32),
        c Nullable(UInt32),
        d Nullable(Tuple(e UInt32, f Nullable(UInt32)))
    ))
) ENGINE = Memory;

INSERT INTO test
SELECT NULL
FROM numbers(4);

SELECT json.d AS path, toTypeName(path) FROM test;

