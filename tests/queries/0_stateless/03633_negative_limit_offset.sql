SET enable_analyzer=0;
SELECT 'Old Analyzer:';

SELECT 'Negative Limit Only';
SELECT number FROM numbers(10) ORDER BY number LIMIT -1;
SELECT number FROM numbers(10) ORDER BY number LIMIT -3;
SELECT number FROM numbers(10) ORDER BY number LIMIT -100;
SELECT number FROM numbers(10) ORDER BY number LIMIT -0;
SELECT number FROM numbers(10) ORDER BY number LIMIT -9223372036854775808;
SELECT number FROM numbers(1000000) ORDER BY number LIMIT -1;

SELECT 'Negative Offset Only';
SELECT number FROM numbers(10) ORDER BY number OFFSET -1;
SELECT number FROM numbers(10) ORDER BY number OFFSET -3;
SELECT number FROM numbers(10) ORDER BY number OFFSET -100;
SELECT number FROM numbers(10) ORDER BY number OFFSET -0;
SELECT number FROM numbers(10) ORDER BY number OFFSET -9223372036854775808;
SELECT number FROM numbers(1000000) ORDER BY number OFFSET -999999;
SELECT number FROM numbers(1000000) OFFSET -1000000;

SELECT 'Negative Limit and Negative Offset';
SELECT number FROM numbers(10) ORDER BY number LIMIT -1 OFFSET -5;
SELECT number FROM numbers(10) ORDER BY number LIMIT -3 OFFSET -9;
SELECT number FROM numbers(10) ORDER BY number LIMIT -2 OFFSET -15;
SELECT number FROM numbers(1000) ORDER BY number LIMIT -5 OFFSET -4;
SELECT number FROM numbers(100000) ORDER BY number LIMIT -5 OFFSET -1000;

SELECT 'Negative Limit and Positive Offset';
SELECT number FROM numbers(10) ORDER BY number LIMIT -1 OFFSET 5;
SELECT number FROM numbers(10) ORDER BY number LIMIT -3 OFFSET 8;
SELECT number FROM numbers(10) ORDER BY number LIMIT -8 OFFSET 3;
SELECT number FROM numbers(100000) LIMIT -5 OFFSET 100000;

SELECT 'Positive Limit and Negative Offset';
SELECT number FROM numbers(10) ORDER BY number LIMIT 1 OFFSET -5;
SELECT number FROM numbers(10) ORDER BY number LIMIT 3 OFFSET -8;
SELECT number FROM numbers(10) ORDER BY number LIMIT 8 OFFSET -3;

SELECT 'Misc';
SELECT DISTINCT number % 8 AS x FROM numbers(120) ORDER BY x LIMIT -3 OFFSET -2;
SELECT DISTINCT number % 80 AS x FROM numbers(120) ORDER BY x LIMIT -3 OFFSET 50;
SELECT * FROM system.numbers_mt WHERE number = 1000000 OFFSET -1;
SELECT * FROM system.numbers_mt WHERE number = 1000000 LIMIT -1;
SELECT * FROM system.numbers_mt WHERE number = 1000000 LIMIT -1 OFFSET -1;
SELECT number FROM numbers(1000000) ORDER BY number LIMIT -1;
SELECT DISTINCT number FROM numbers(1000000) ORDER BY number LIMIT -1 OFFSET -999999;
SELECT number FROM numbers(1000000) ORDER BY number LIMIT -1 OFFSET -999999;
SELECT DISTINCT number FROM numbers(20) LIMIT 18446744073709551615 OFFSET -446744073709551615;

SELECT 'Double Column';
DROP TABLE IF EXISTS num_tab;
CREATE TABLE num_tab
(
    `id` UInt8,
    `val` UInt32
)
ENGINE = MergeTree
ORDER BY (id, val)
AS SELECT
    number % 2 AS id,
    number AS val
FROM numbers(20);

SELECT if((count() = 5) AND (min(val) = 15) AND (max(val) = 19) AND (sum(val) = 85) AND (uniqExact(id) = 2), 'OK', 'FAIL')
FROM
(
    SELECT
        id,
        val
    FROM num_tab ORDER BY val ASC LIMIT -5
);

SELECT 'Big Tables';
DROP TABLE IF EXISTS num_tab;
CREATE TABLE num_tab
ENGINE = MergeTree
ORDER BY number
AS SELECT number FROM numbers(1000000);

DROP TABLE IF EXISTS modified_tab;
CREATE TABLE modified_tab ENGINE=MergeTree()
ORDER BY number 
AS SELECT number FROM
(SELECT number FROM num_tab ORDER BY number OFFSET -10);

SELECT count(number), sum(number) FROM modified_tab;


DROP TABLE IF EXISTS num_tab;
CREATE TABLE num_tab
ENGINE = MergeTree
ORDER BY number
AS SELECT number FROM numbers(1000000);

DROP TABLE IF EXISTS modified_tab;
CREATE TABLE modified_tab ENGINE=MergeTree()
ORDER BY number 
AS SELECT number FROM
(SELECT number FROM num_tab ORDER BY number LIMIT -10 OFFSET -100000);

SELECT number FROM modified_tab;

SET enable_analyzer=1;
SELECT 'New Analyzer:';
SELECT 'Negative Limit Only';
SELECT number FROM numbers(10) ORDER BY number LIMIT -1;
SELECT number FROM numbers(10) ORDER BY number LIMIT -3;
SELECT number FROM numbers(10) ORDER BY number LIMIT -100;
SELECT number FROM numbers(10) ORDER BY number LIMIT -0;
SELECT number FROM numbers(10) ORDER BY number LIMIT -9223372036854775808;
SELECT number FROM numbers(1000000) ORDER BY number LIMIT -1;

SELECT 'Negative Offset Only';
SELECT number FROM numbers(10) ORDER BY number OFFSET -1;
SELECT number FROM numbers(10) ORDER BY number OFFSET -3;
SELECT number FROM numbers(10) ORDER BY number OFFSET -100;
SELECT number FROM numbers(10) ORDER BY number OFFSET -0;
SELECT number FROM numbers(10) ORDER BY number OFFSET -9223372036854775808;
SELECT number FROM numbers(1000000) ORDER BY number OFFSET -999999;
SELECT number FROM numbers(1000000) OFFSET -1000000;

SELECT 'Negative Limit and Negative Offset';
SELECT number FROM numbers(10) ORDER BY number LIMIT -1 OFFSET -5;
SELECT number FROM numbers(10) ORDER BY number LIMIT -3 OFFSET -9;
SELECT number FROM numbers(10) ORDER BY number LIMIT -2 OFFSET -15;
SELECT number FROM numbers(1000) ORDER BY number LIMIT -5 OFFSET -4;
SELECT number FROM numbers(100000) ORDER BY number LIMIT -5 OFFSET -1000;

SELECT 'Negative Limit and Positive Offset';
SELECT number FROM numbers(10) ORDER BY number LIMIT -1 OFFSET 5;
SELECT number FROM numbers(10) ORDER BY number LIMIT -3 OFFSET 8;
SELECT number FROM numbers(10) ORDER BY number LIMIT -8 OFFSET 3;
SELECT number FROM numbers(100000) LIMIT -5 OFFSET 100000;

SELECT 'Positive Limit and Negative Offset';
SELECT number FROM numbers(10) ORDER BY number LIMIT 1 OFFSET -5;
SELECT number FROM numbers(10) ORDER BY number LIMIT 3 OFFSET -8;
SELECT number FROM numbers(10) ORDER BY number LIMIT 8 OFFSET -3;

SELECT 'Misc';
SELECT DISTINCT number % 8 AS x FROM numbers(120) ORDER BY x LIMIT -3 OFFSET -2;
SELECT DISTINCT number % 80 AS x FROM numbers(120) ORDER BY x LIMIT -3 OFFSET 50;
SELECT * FROM system.numbers_mt WHERE number = 1000000 OFFSET -1;
SELECT * FROM system.numbers_mt WHERE number = 1000000 LIMIT -1;
SELECT * FROM system.numbers_mt WHERE number = 1000000 LIMIT -1 OFFSET -1;
SELECT number FROM numbers(1000000) ORDER BY number LIMIT -1;
SELECT DISTINCT number FROM numbers(1000000) ORDER BY number LIMIT -1 OFFSET -999999;
SELECT number FROM numbers(1000000) ORDER BY number LIMIT -1 OFFSET -999999;
SELECT DISTINCT number FROM numbers(20) LIMIT 18446744073709551615 OFFSET -446744073709551615;

SELECT 'Double Column';
DROP TABLE IF EXISTS num_tab;
CREATE TABLE num_tab
(
    `id` UInt8,
    `val` UInt32
)
ENGINE = MergeTree
ORDER BY (id, val)
AS SELECT
    number % 2 AS id,
    number AS val
FROM numbers(20);

SELECT if((count() = 5) AND (min(val) = 15) AND (max(val) = 19) AND (sum(val) = 85) AND (uniqExact(id) = 2), 'OK', 'FAIL')
FROM
(
    SELECT
        id,
        val
    FROM num_tab ORDER BY val ASC LIMIT -5
);

SELECT 'Big Tables';
DROP TABLE IF EXISTS num_tab;
CREATE TABLE num_tab
ENGINE = MergeTree
ORDER BY number
AS SELECT number FROM numbers(1000000);

DROP TABLE IF EXISTS modified_tab;
CREATE TABLE modified_tab ENGINE=MergeTree()
ORDER BY number 
AS SELECT number FROM
(SELECT number FROM num_tab ORDER BY number OFFSET -10);

SELECT count(number), sum(number) FROM modified_tab;


DROP TABLE IF EXISTS num_tab;
CREATE TABLE num_tab
ENGINE = MergeTree
ORDER BY number
AS SELECT number FROM numbers(1000000);

DROP TABLE IF EXISTS modified_tab;
CREATE TABLE modified_tab ENGINE=MergeTree()
ORDER BY number 
AS SELECT number FROM
(SELECT number FROM num_tab ORDER BY number LIMIT -10 OFFSET -100000);

SELECT number FROM modified_tab;

SELECT DISTINCT number
FROM (SELECT number FROM numbers_mt(1000000) LIMIT -214748)
WHERE 0;
