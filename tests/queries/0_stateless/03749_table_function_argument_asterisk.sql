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
