SELECT avg(number) AS number, max(number) FROM numbers(10); -- { serverError 184 }
SELECT sum(x) AS x, max(x) FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) t; -- { serverError 184 }
select sum(C1) as C1, count(C1) as C2 from (select number as C1 from numbers(3)) as ITBL; -- { serverError 184 }

set prefer_column_name_to_alias  = 1;
SELECT avg(number) AS number, max(number) FROM numbers(10);
SELECT sum(x) AS x, max(x) FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) t settings prefer_column_name_to_alias = 1;
select sum(C1) as C1, count(C1) as C2 from (select number as C1 from numbers(3)) as ITBL settings prefer_column_name_to_alias = 1;
