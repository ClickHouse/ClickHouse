SET max_rows_to_sort = 100;
SELECT DISTINCT x FROM (SELECT number % 10 AS x FROM system.numbers LIMIT 100000) ORDER BY x;
