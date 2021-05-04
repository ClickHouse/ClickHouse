-- { echo }
set allow_experimental_window_functions = 1;

select row_number() over (order by dummy) from (select * from remote('127.0.0.{1,2}', system, one));

select row_number() over (order by dummy) from remote('127.0.0.{1,2}', system, one);

select max(identity(dummy + 1)) over () from remote('127.0.0.{1,2}', system, one);

drop table if exists t_01568;

create table t_01568 engine Log as select intDiv(number, 3) p, number from numbers(9);

select sum(number) over w, max(number) over w from t_01568 window w as (partition by p);

select sum(number) over w, max(number) over w from remote('127.0.0.{1,2}', '', t_01568) window w as (partition by p);

select distinct sum(number) over w, max(number) over w from remote('127.0.0.{1,2}', '', t_01568) window w as (partition by p);

-- window functions + aggregation w/shards
select groupArray(groupArray(number)) over (rows unbounded preceding) from remote('127.0.0.{1,2}', '', t_01568) group by mod(number, 3);

drop table t_01568;
