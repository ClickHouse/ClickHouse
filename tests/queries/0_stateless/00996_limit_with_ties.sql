DROP TABLE IF EXISTS ties;
CREATE TABLE ties (a Int) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO ties VALUES (1), (1), (2), (2), (2), (2) (3), (3);

SELECT a FROM ties order by a limit 1 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 3 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 5 with ties;
SELECT '*';

SET max_block_size = 2;
SELECT a FROM ties order by a limit 1, 1 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 1, 2 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 2 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 2, 3 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 4 with ties;
SELECT '*';

SET max_block_size = 3;
SELECT a FROM ties order by a limit 1 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 2, 3 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 3, 2 with ties;
SELECT '*';

select count() from (select number > 100 from numbers(2000) order by number > 100 limit 1, 7 with ties);    --TODO replace "number > 100" with "number > 100 as n"
select count() from (select number, number < 100 from numbers(2000) order by number < 100 desc limit 10 with ties);
SET max_block_size = 5;
select count() from (select number < 100, number from numbers(2000) order by number < 100 desc limit 10 with ties);

SELECT count() FROM (WITH data AS (
    SELECT * FROM numbers(0, 10)
    UNION ALL
    SELECT * FROM numbers(10, 10)
)
SELECT number div 10 AS ten, number
FROM data
ORDER BY ten
LIMIT 8,6 WITH TIES);

SELECT count() FROM (WITH data AS (
    SELECT * FROM numbers(0, 10)
    UNION ALL
    SELECT * FROM numbers(10, 10)
)
SELECT number div 11 AS eleven, number
FROM data
ORDER BY eleven
LIMIT 8,6 WITH TIES);

DROP TABLE ties;
