SELECT
    0 OR NULL,
    1 OR NULL,
    toNullable(0) OR NULL,
    toNullable(1) OR NULL,
    0.0 OR NULL,
    0.1 OR NULL,
    NULL OR 1 OR NULL,
    0 OR NULL OR 1 OR NULL;
   
SELECT
    x,
    0 OR x,
    1 OR x,
    x OR x,
    toNullable(0) OR x,
    toNullable(1) OR x,
    0.0 OR x,
    0.1 OR x,
    x OR 1 OR x,
    0 OR x OR 1 OR x
FROM (SELECT number % 2 ? number % 3 : NULL AS x FROM system.numbers LIMIT 10);

SELECT
    x,
    0 AND x,
    1 AND x,
    x AND x,
    toNullable(0) AND x,
    toNullable(1) AND x,
    0.0 AND x,
    0.1 AND x,
    x AND 1 AND x,
    0 AND x AND 1 AND x
FROM (SELECT number % 2 ? number % 3 : NULL AS x FROM system.numbers LIMIT 10);

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    x Nullable(Int32)
) ENGINE = Log;

INSERT INTO test VALUES(1), (0), (null);

SELECT * FROM test;
SELECT x FROM test WHERE x != 0;
SELECT x FROM test WHERE x != 0 OR isNull(x);
SELECT x FROM test WHERE x != 1;

DROP TABLE test;
