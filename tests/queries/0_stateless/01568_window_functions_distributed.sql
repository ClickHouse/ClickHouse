-- Tags: distributed

-- { echo }
select row_number() over (order by dummy) as x from (select * from remote('127.0.0.{1,2}', system, one)) order by x;

select row_number() over (order by dummy) as x from remote('127.0.0.{1,2}', system, one) order by x;

select max(identity(dummy + 1)) over () as x from remote('127.0.0.{1,2}', system, one) order by x;

drop table if exists t_01568;

create table t_01568 engine Memory as
select intDiv(number, 3) p, modulo(number, 3) o, number
from numbers(9);

select sum(number) over w as x, max(number) over w as y from t_01568 window w as (partition by p) order by x, y;

select sum(number) over w, max(number) over w from t_01568 window w as (partition by p) order by p;

select sum(number) over w as x, max(number) over w as y from remote('127.0.0.{1,2}', '', t_01568) window w as (partition by p) order by x, y;

select sum(number) over w as x, max(number) over w as y from remote('127.0.0.{1,2}', '', t_01568) window w as (partition by p) order by x, y SETTINGS max_threads = 1;

select distinct sum(number) over w as x, max(number) over w as y from remote('127.0.0.{1,2}', '', t_01568) window w as (partition by p) order by x, y;

-- window functions + aggregation w/shards
select groupArray(groupArray(number)) over (rows unbounded preceding) as x from remote('127.0.0.{1,2}', '', t_01568) group by mod(number, 3) order by x;
select groupArray(groupArray(number)) over (rows unbounded preceding) as x from remote('127.0.0.{1,2}', '', t_01568) group by mod(number, 3) order by x settings distributed_group_by_no_merge=1;
select groupArray(groupArray(number)) over (rows unbounded preceding) as x from remote('127.0.0.{1,2}', '', t_01568) group by mod(number, 3) order by x settings distributed_group_by_no_merge=2; -- { serverError NOT_IMPLEMENTED }

-- proper ORDER BY w/window functions
select p, o, count() over (partition by p)
from remote('127.0.0.{1,2}', '',  t_01568)
order by p, o;

drop table t_01568;
