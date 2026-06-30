DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    k UInt64,
)
ENGINE = MergeTree
ORDER BY k SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO test_table SELECT number FROM numbers(100000);

SET enable_analyzer = 1;

SELECT * FROM test_table WHERE k in (100) = 1;
SELECT * FROM test_table WHERE k = (100) = 1;
SELECT * FROM test_table WHERE k not in (100) = 0;
SELECT * FROM test_table WHERE k != (100) = 0;
SELECT * FROM test_table WHERE 1 = (k = 100);
SELECT * FROM test_table WHERE 0 = (k not in (100));
SELECT * FROM test_table WHERE k < 1 = 1;
SELECT * FROM test_table WHERE k >= 1 = 0;
SELECT * FROM test_table WHERE k > 1 = 0;
SELECT * FROM test_table WHERE ((k not in (101) = 0) OR (k in (100) = 1)) = 1;
SELECT * FROM test_table WHERE (NOT ((k not in (100) = 0) OR (k in (100) = 1))) = 0;
SELECT * FROM test_table WHERE (NOT ((k in (101) = 0) OR (k in (100) = 1))) = 1;
SELECT * FROM test_table WHERE ((k not in (101) = 0) OR (k in (100) = 1)) = 1;
SELECT * FROM test_table WHERE ((k not in (99) = 1) AND (k in (100) = 1)) = 1;
-- we skip optimizing queries with toNullable(0 or 1) but lets make sure they still work
SELECT * FROM test_table WHERE (k = 101) = toLowCardinality(toNullable(1));
SELECT * FROM test_table WHERE (k = 101) = toNullable(1);
SELECT * FROM test_table WHERE (k = 101) = toLowCardinality(1);
SELECT * FROM test_table WHERE ((k not in (101) = toNullable(0)) OR (k in (100) = toNullable(1))) = toNullable(1);
SELECT * FROM test_table WHERE (((k NOT IN toLowCardinality(toNullable(101))) = toLowCardinality(toNullable(0))) OR ((k IN (toLowCardinality(100))) = toNullable(1)));
SELECT * FROM test_table WHERE (((k IN toLowCardinality(toNullable(101))) = toLowCardinality(toNullable(0))) AND ((k NOT IN (toLowCardinality(100))) = toNullable(1))) = toNullable(toLowCardinality(0));

SELECT count()
FROM
(
    EXPLAIN PLAN indexes=1
    SELECT * FROM test_table WHERE k in (100) = 1
)
WHERE
    explain LIKE '%Granules: 1/%';

SELECT count()
FROM
(
    EXPLAIN PLAN indexes=1
    SELECT * FROM test_table WHERE k >= 1 = 0
)
WHERE
    explain LIKE '%Granules: 1/%';

SELECT count()
FROM
(
    EXPLAIN PLAN indexes=1
    SELECT * FROM test_table WHERE k not in (100) = 0
)
WHERE
    explain LIKE '%Granules: 1/%';

SELECT count()
FROM
(
    EXPLAIN PLAN indexes=1
    SELECT * FROM test_table WHERE k > 1 = 0
)
WHERE
    explain LIKE '%Granules: 1/%';

SELECT count()
FROM
(
    EXPLAIN PLAN indexes=1
    SELECT * FROM test_table WHERE (NOT ((k not in (100) = 0) OR (k in (100) = 1))) = 0
)
WHERE
    explain LIKE '%Granules: 1/%';


SELECT count()
FROM
(
    EXPLAIN PLAN indexes=1
    SELECT * FROM test_table WHERE (NOT ((k in (101) = 0) OR (k in (100) = 1))) = 1
)
WHERE
    explain LIKE '%Granules: 1/%';


DROP TABLE test_table;
