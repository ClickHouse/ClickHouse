-- https://github.com/ClickHouse/ClickHouse/issues/11813
SET allow_experimental_analyzer=1;
select 1 from (select 1 x) l join (select 1 y, [1] a) r on l.x = r.y array join r.a;
