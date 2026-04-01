-- Basic SHUFFLE
SELECT count() FROM (SELECT * FROM numbers(100) SHUFFLE);

-- SHUFFLE LIMIT
SELECT count() FROM (SELECT * FROM numbers(1000) SHUFFLE LIMIT 10);

-- SHUFFLE returns all rows without LIMIT
SELECT count() FROM (SELECT * FROM numbers(100) SHUFFLE) WHERE number < 200;

-- SHUFFLE LIMIT does not return more than LIMIT
SELECT count() FROM (SELECT * FROM numbers(1000) SHUFFLE LIMIT 5);

-- SHUFFLE on empty table
SELECT count() FROM (SELECT * FROM numbers(0) SHUFFLE);

-- SHUFFLE LIMIT greater than number of rows
SELECT count() FROM (SELECT * FROM numbers(10) SHUFFLE LIMIT 100);

-- SHUFFLE with WHERE
SELECT count() FROM (SELECT * FROM numbers(100) WHERE number < 50 SHUFFLE LIMIT 10);

-- SHUFFLE with multithreaded source
SELECT count() FROM (SELECT * FROM numbers_mt(10000) SHUFFLE LIMIT 100) SETTINGS max_threads=4;

-- SHUFFLE on MergeTree table
DROP TABLE IF EXISTS test_shuffle_basic;
CREATE TABLE test_shuffle_basic (id UInt64, value String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test_shuffle_basic SELECT number, toString(number) FROM numbers(1000);
SELECT count() FROM (SELECT * FROM test_shuffle_basic SHUFFLE LIMIT 10);
DROP TABLE test_shuffle_basic;

-- SHUFFLE does not lose or duplicate rows
SELECT count(DISTINCT number) FROM (SELECT * FROM numbers(100) SHUFFLE);

-- SHUFFLE LIMIT returns unique rows
SELECT count(DISTINCT number) FROM (SELECT * FROM numbers(1000) SHUFFLE LIMIT 100);

-- SHUFFLE results differ between runs (randomness check)
SELECT uniqExact(s) > 1 FROM (
    SELECT arraySort(groupArray(number)) as s FROM (SELECT * FROM numbers(100) SHUFFLE LIMIT 5)
    UNION ALL
    SELECT arraySort(groupArray(number)) FROM (SELECT * FROM numbers(100) SHUFFLE LIMIT 5)
    UNION ALL
    SELECT arraySort(groupArray(number)) FROM (SELECT * FROM numbers(100) SHUFFLE LIMIT 5)
    UNION ALL
    SELECT arraySort(groupArray(number)) FROM (SELECT * FROM numbers(100) SHUFFLE LIMIT 5)
    UNION ALL
    SELECT arraySort(groupArray(number)) FROM (SELECT * FROM numbers(100) SHUFFLE LIMIT 5)
);

-- SHUFFLE with GROUP BY
SELECT count() FROM (SELECT number % 10 as n, count() as c FROM numbers(1000) GROUP BY n SHUFFLE);

-- SHUFFLE with JOIN
SELECT count() FROM (
    SELECT a.number FROM numbers(100) a
    INNER JOIN numbers(50) b ON a.number = b.number
    SHUFFLE LIMIT 10
);

-- SHUFFLE in subquery
SELECT count() FROM (
    SELECT * FROM (SELECT * FROM numbers(100) SHUFFLE LIMIT 50) SHUFFLE LIMIT 10
);

-- SHUFFLE with DISTINCT
SELECT count() FROM (SELECT DISTINCT number % 20 as n FROM numbers(100) SHUFFLE);

-- SHUFFLE with aggregate in subquery
SELECT count() FROM (
    SELECT sum(number) as s FROM numbers(1000) GROUP BY number % 10 SHUFFLE LIMIT 5
);
