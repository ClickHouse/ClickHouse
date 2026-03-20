-- { echo }

SET enable_analyzer=0;
SELECT 'Old Analyzer:';

SELECT 'Negative Limit Only';
SELECT number FROM numbers(10) ORDER BY number LIMIT -1 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -3 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -100 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -0 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -9223372036854775808 WITH TIES;
SELECT number FROM numbers(1000000) ORDER BY number LIMIT -1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -3 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -100 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -0 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(1000000) ORDER BY x LIMIT -1 WITH TIES;

SELECT 'Negative Offset Only (with LIMIT 0 WITH TIES)';
SELECT number FROM numbers(10) ORDER BY number LIMIT -1, 0 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -3, 0 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -100, 0 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -0, 0 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -1, 0 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -3, 0 WITH TIES;

SELECT 'Negative Limit and Negative Offset';
SELECT number FROM numbers(10) ORDER BY number LIMIT -5, -1 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -9, -3 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -15, -2 WITH TIES;
SELECT number FROM numbers(1000) ORDER BY number LIMIT -4, -5 WITH TIES;
SELECT number FROM numbers(100000) ORDER BY number LIMIT -1000, -5 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -5, -1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -9, -3 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -15, -2 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(1000) ORDER BY x LIMIT -4, -5 WITH TIES;

SELECT 'Negative Limit and Positive Offset';
SELECT number FROM numbers(10) ORDER BY number LIMIT 5, -1 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT 8, -3 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT 3, -8 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT 5, -1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT 8, -3 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT 3, -8 WITH TIES;

SELECT 'Positive Limit and Negative Offset';
SELECT number FROM numbers(10) ORDER BY number LIMIT -5, 1 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -8, 3 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -3, 8 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -5, 1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -8, 3 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -3, 8 WITH TIES;

SELECT 'Misc';
SELECT DISTINCT number % 8 AS x FROM numbers(120) ORDER BY x LIMIT -2, -3 WITH TIES;
SELECT DISTINCT number % 80 AS x FROM numbers(120) ORDER BY x LIMIT 50, -3 WITH TIES;
SELECT * FROM system.numbers_mt WHERE number = 1000000 ORDER BY number LIMIT -1, 0 WITH TIES;
SELECT * FROM system.numbers_mt WHERE number = 1000000 ORDER BY number LIMIT -1 WITH TIES;
SELECT * FROM system.numbers_mt WHERE number = 1000000 ORDER BY number LIMIT -1, -1 WITH TIES;
SELECT number FROM numbers(1000000) ORDER BY number LIMIT -1 WITH TIES;
SELECT DISTINCT number FROM numbers(1000000) ORDER BY number LIMIT -999999, -1 WITH TIES;
SELECT number FROM numbers(1000000) ORDER BY number LIMIT -999999, -1 WITH TIES;
SELECT DISTINCT intDiv(number, 2) AS x FROM numbers(20) ORDER BY x LIMIT -2, -3 WITH TIES;
SELECT intDiv(number, 500000) AS x FROM system.numbers_mt WHERE number <= 1000000 ORDER BY x LIMIT -1 WITH TIES;

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
    FROM num_tab ORDER BY val ASC LIMIT -5 WITH TIES
);

SELECT if((count() = 10) AND (uniqExact(id) = 1), 'OK', 'FAIL')
FROM
(
    SELECT
        id,
        val
    FROM num_tab ORDER BY id ASC LIMIT -5 WITH TIES
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
(SELECT number FROM num_tab ORDER BY number LIMIT -10, 0 WITH TIES);

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
(SELECT number FROM num_tab ORDER BY number LIMIT -100000, -10 WITH TIES);

SELECT number FROM modified_tab;

DROP TABLE IF EXISTS modified_tab;
CREATE TABLE modified_tab ENGINE=MergeTree()
ORDER BY x
AS SELECT intDiv(number, 2) AS x FROM
(SELECT number, intDiv(number, 2) AS x FROM num_tab ORDER BY x, number LIMIT -100000, -10 WITH TIES);

SELECT x FROM modified_tab ORDER BY x;

SELECT 'Small block size';
SET max_block_size = 2;
SELECT number FROM numbers(10) ORDER BY number LIMIT -3 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -3 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -5, -1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -9, -3 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -5, 1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -5, 1 WITH TIES;
SELECT 0 AS x FROM numbers(7) ORDER BY x LIMIT -2 WITH TIES;
SELECT intDiv(number, 3) AS x FROM numbers(12) ORDER BY x LIMIT -4 WITH TIES;
SET max_block_size = 65536;

DROP TABLE IF EXISTS num_tab;
DROP TABLE IF EXISTS modified_tab;

SET enable_analyzer=1;
SELECT 'Analyzer:';

SELECT 'Negative Limit Only';
SELECT number FROM numbers(10) ORDER BY number LIMIT -1 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -3 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -100 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -0 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -9223372036854775808 WITH TIES;
SELECT number FROM numbers(1000000) ORDER BY number LIMIT -1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -3 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -100 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -0 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(1000000) ORDER BY x LIMIT -1 WITH TIES;

SELECT 'Negative Offset Only (with LIMIT 0 WITH TIES)';
SELECT number FROM numbers(10) ORDER BY number LIMIT -1, 0 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -3, 0 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -100, 0 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -0, 0 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -1, 0 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -3, 0 WITH TIES;

SELECT 'Negative Limit and Negative Offset';
SELECT number FROM numbers(10) ORDER BY number LIMIT -5, -1 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -9, -3 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -15, -2 WITH TIES;
SELECT number FROM numbers(1000) ORDER BY number LIMIT -4, -5 WITH TIES;
SELECT number FROM numbers(100000) ORDER BY number LIMIT -1000, -5 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -5, -1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -9, -3 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -15, -2 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(1000) ORDER BY x LIMIT -4, -5 WITH TIES;

SELECT 'Negative Limit and Positive Offset';
SELECT number FROM numbers(10) ORDER BY number LIMIT 5, -1 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT 8, -3 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT 3, -8 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT 5, -1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT 8, -3 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT 3, -8 WITH TIES;

SELECT 'Positive Limit and Negative Offset';
SELECT number FROM numbers(10) ORDER BY number LIMIT -5, 1 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -8, 3 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -3, 8 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -5, 1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -8, 3 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -3, 8 WITH TIES;

SELECT 'Misc';
SELECT DISTINCT number % 8 AS x FROM numbers(120) ORDER BY x LIMIT -2, -3 WITH TIES;
SELECT DISTINCT number % 80 AS x FROM numbers(120) ORDER BY x LIMIT 50, -3 WITH TIES;
SELECT * FROM system.numbers_mt WHERE number = 1000000 ORDER BY number LIMIT -1, 0 WITH TIES;
SELECT * FROM system.numbers_mt WHERE number = 1000000 ORDER BY number LIMIT -1 WITH TIES;
SELECT * FROM system.numbers_mt WHERE number = 1000000 ORDER BY number LIMIT -1, -1 WITH TIES;
SELECT number FROM numbers(1000000) ORDER BY number LIMIT -1 WITH TIES;
SELECT DISTINCT number FROM numbers(1000000) ORDER BY number LIMIT -999999, -1 WITH TIES;
SELECT number FROM numbers(1000000) ORDER BY number LIMIT -999999, -1 WITH TIES;
SELECT DISTINCT intDiv(number, 2) AS x FROM numbers(20) ORDER BY x LIMIT -2, -3 WITH TIES;
SELECT intDiv(number, 500000) AS x FROM system.numbers_mt WHERE number <= 1000000 ORDER BY x LIMIT -1 WITH TIES;

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
    FROM num_tab ORDER BY val ASC LIMIT -5 WITH TIES
);

SELECT if((count() = 10) AND (uniqExact(id) = 1), 'OK', 'FAIL')
FROM
(
    SELECT
        id,
        val
    FROM num_tab ORDER BY id ASC LIMIT -5 WITH TIES
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
(SELECT number FROM num_tab ORDER BY number LIMIT -10, 0 WITH TIES);

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
(SELECT number FROM num_tab ORDER BY number LIMIT -100000, -10 WITH TIES);

SELECT number FROM modified_tab;

DROP TABLE IF EXISTS modified_tab;
CREATE TABLE modified_tab ENGINE=MergeTree()
ORDER BY x
AS SELECT intDiv(number, 2) AS x FROM
(SELECT number, intDiv(number, 2) AS x FROM num_tab ORDER BY x, number LIMIT -100000, -10 WITH TIES);

SELECT x FROM modified_tab ORDER BY x;

SELECT 'Small block size';
SET max_block_size = 2;
SELECT number FROM numbers(10) ORDER BY number LIMIT -3 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -3 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -5, -1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -9, -3 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT -5, 1 WITH TIES;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -5, 1 WITH TIES;
SELECT 0 AS x FROM numbers(7) ORDER BY x LIMIT -2 WITH TIES;
SELECT intDiv(number, 3) AS x FROM numbers(12) ORDER BY x LIMIT -4 WITH TIES;
SET max_block_size = 65536;

DROP TABLE IF EXISTS num_tab;
DROP TABLE IF EXISTS modified_tab;
