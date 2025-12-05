SELECT *
FROM remote(*, '127.{1,2}', view(
    SELECT 2
)); -- { serverError BAD_ARGUMENTS }

SELECT *
FROM remote(*, view(
    SELECT 2
));  -- { serverError BAD_ARGUMENTS }

SELECT *
FROM remote(*, '127.{1,2}', view(
    SELECT toLowCardinality(2)
));  -- { serverError BAD_ARGUMENTS }

SELECT *
FROM remote(*, '127.{1,2}', view(
    SELECT 1
    FROM numbers(1)
    GROUP BY toLowCardinality(2)
));  -- { serverError BAD_ARGUMENTS }

SELECT DISTINCT '/01650_drop_part_and_deduplication_partitioned_table/blocks/', 60, k1 
FROM remote(*, '127.{1,2}', view(SELECT 1 AS k1, 65535, 2 AS k2, 3 AS v
FROM numbers(2, cityHash64(k1)) 
WHERE toLowCardinality(60) GROUP BY GROUPING SETS ((toLowCardinality(2))) 
HAVING equals(k1, toNullable(60)))) FINAL; -- { serverError BAD_ARGUMENTS }

SELECT * FROM numbers(*, 2); -- { serverError BAD_ARGUMENTS }

SELECT * FROM numbers(2, *); -- { serverError BAD_ARGUMENTS }

SELECT * FROM numbers_mt(2, *); -- { serverError BAD_ARGUMENTS }

SELECT *
FROM generateSeries(*, 1, 3); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS logs_2025_01;
DROP TABLE IF EXISTS logs_2025_02;
DROP TABLE IF EXISTS logs_2025_03;

CREATE TABLE logs_2025_01 (date Date, message String) ENGINE = MergeTree ORDER BY date;
CREATE TABLE logs_2025_02 (date Date, message String) ENGINE = MergeTree ORDER BY date;
CREATE TABLE logs_2025_03 (date Date, message String) ENGINE = MergeTree ORDER BY date;

INSERT INTO logs_2025_01 VALUES ('2025-01-15', 'January log');
INSERT INTO logs_2025_02 VALUES ('2025-02-15', 'February log');
INSERT INTO logs_2025_03 VALUES ('2025-03-15', 'March log');

SELECT * FROM merge(*, currentDatabase(), '^logs_2025_.*'); -- { serverError BAD_ARGUMENTS }
