-- https://github.com/ClickHouse/ClickHouse/issues/10276
SET enable_analyzer=1;
SELECT
    sum(x.n) as n,
    sum(z.n) as n2
FROM
(
    SELECT 1000 AS n,1 as id
) AS x
join (select 10000 as n,1 as id) as y
on x.id = y.id
left join (select 100000 as n,1 as id) as z
on x.id = z.id;
