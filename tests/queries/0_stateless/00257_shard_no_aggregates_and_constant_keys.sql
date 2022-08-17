select 40 as z from (select * from system.numbers limit 3) group by z;
select 41 as z from remote('127.0.0.{2,3}', system.one) group by z;
select count(), 42 AS z from remote('127.0.0.{2,3}', system.one) group by z;
select 43 AS z from remote('127.0.0.{2,3}', system.one) group by 42, 43, 44;
select 11 AS z from (SELECT 2 UNION ALL SELECT 3) group by 42, 43, 44;

select 40 as z from (select * from system.numbers limit 3) group by z WITH TOTALS;
select 41 as z from remote('127.0.0.{2,3}', system.one) group by z WITH TOTALS;
select count(), 42 AS z from remote('127.0.0.{2,3}', system.one) group by z WITH TOTALS;
select 43 AS z from remote('127.0.0.{2,3}', system.one) group by 42, 43, 44 WITH TOTALS;
select 11 AS z from (SELECT 1 UNION ALL SELECT 2) group by 42, 43, 44 WITH TOTALS;
select 11 AS z from (SELECT 2 UNION ALL SELECT 3) group by 42, 43, 44 WITH TOTALS;

SELECT count() WITH TOTALS;
SELECT count() FROM remote('127.0.0.{2,3}', system.one) WITH TOTALS;
