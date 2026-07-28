-- Tags: distributed

-- { echo }

DROP TABLE IF EXISTS test;
CREATE TABLE test (id UInt64) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test SELECT number FROM numbers(200000);

SELECT 'Negative LIMIT only';
SELECT count(), min(x), max(x)
FROM
(
    SELECT intDiv(id, 100) AS x
    FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test)
    ORDER BY x
    LIMIT -250 WITH TIES
);

SELECT 'Negative LIMIT and negative OFFSET';
SELECT count(), min(x), max(x)
FROM
(
    SELECT intDiv(id, 100) AS x
    FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test)
    ORDER BY x
    LIMIT -50, -250 WITH TIES
);

SELECT 'Positive LIMIT and negative OFFSET';
SELECT count(), min(x), max(x)
FROM
(
    SELECT intDiv(id, 100) AS x
    FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test)
    ORDER BY x
    LIMIT -50, 250 WITH TIES
);

DROP TABLE IF EXISTS test;
