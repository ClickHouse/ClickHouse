select s.a as a, s.a + 1 as b from (select 10 as a) s;
select s.a + 1 as a, s.a as b from (select 10 as a) s;
select s.a + 1 as a, s.a + 1 as b from (select 10 as a) s;
select s.a + 1 as b, s.a + 2 as a from (select 10 as a) s;
select s.a + 2 as b, s.a + 1 as a from (select 10 as a) s;

SELECT 0 as t FROM (SELECT 1 as t) as inn WHERE inn.t = 1;
SELECT sum(value) as value FROM (SELECT 1 as value) as data WHERE data.value > 0;
