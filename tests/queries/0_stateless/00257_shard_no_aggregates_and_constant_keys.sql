-- Tags: shard

set enable_analyzer = 1;
set enable_positional_arguments = 0;

select 40 as z from (select * from system.numbers limit 3) group by z;
select 41 as z from remote('127.0.0.{2,3}', system.one) group by z;
select count(), 42 AS z from remote('127.0.0.{2,3}', system.one) group by z;
select 43 AS z from remote('127.0.0.{2,3}', system.one) group by 42, 43, 44;
select 11 AS z from (SELECT 2 UNION ALL SELECT 3) group by 42, 43, 44;

select 40 as z from (select * from system.numbers limit 3) group by z WITH TOTALS;
-- NOTE: non-analyzer preserves the original header (i.e. 41) for TOTALS in
-- case of remote queries with GROUP BY some_requested_const and there were no
-- aggregate functions, the query above. But everything else works in the same
-- way, i.e.:
--
--     select 41 as z, count() from remote('127.0.0.{2,3}', system.one) group by z WITH TOTALS;
--     select 41 as z from remote('127.0.0.{2,3}', system.one) group by 1 WITH TOTALS;
--
select 41 as z from remote('127.0.0.{2,3}', system.one) group by z WITH TOTALS;
select count(), 42 AS z from remote('127.0.0.{2,3}', system.one) group by z WITH TOTALS;
select 43 AS z from remote('127.0.0.{2,3}', system.one) group by 42, 43, 44 WITH TOTALS;
select 11 AS z from (SELECT 1 UNION ALL SELECT 2) group by 42, 43, 44 WITH TOTALS;
select 11 AS z from (SELECT 2 UNION ALL SELECT 3) group by 42, 43, 44 WITH TOTALS;

SELECT count() WITH TOTALS;
SELECT count() FROM remote('127.0.0.{2,3}', system.one) WITH TOTALS;
