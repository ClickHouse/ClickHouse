-- Tags: distributed

DROP TABLE IF EXISTS test;
CREATE TABLE test (id UInt64) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test SELECT number FROM numbers(200000);

SELECT
    concat(current_database(), '')
FROM
(
    SELECT id
    FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test)
    ORDER BY id
    LIMIT -2147483648 WITH TIES
)
ORDER BY ALL
FORMAT Null;

SELECT
    concat(current_database(), '')
FROM
(
    SELECT id
    FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test)
    ORDER BY id
    LIMIT -2, 0 WITH TIES
)
ORDER BY ALL
FORMAT Null;

SELECT
    concat(current_database(), '')
FROM
(
    SELECT intDiv(id, 100) AS x
    FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test)
    ORDER BY x
    LIMIT -2147483648 WITH TIES
)
ORDER BY ALL
FORMAT Null;

DROP TABLE IF EXISTS test;
