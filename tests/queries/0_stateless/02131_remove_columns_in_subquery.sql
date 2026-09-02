select count(1) from (SELECT 1 AS a, count(1) FROM numbers(5));
select count(1) from (SELECT 1 AS a, count(1) + 1 FROM numbers(5));