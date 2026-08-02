drop table if exists test1;

create table test1 (i int, j int) engine MergeTree partition by i order by tuple() settings index_granularity = 1;

insert into test1 select number, number + 100 from numbers(10);
select count() from test1 where i not in (1,2,3);
set max_rows_to_read = 5;
select * from test1 where i not in (1,2,3,4,5) order by i;

drop table test1;

drop table if exists t1;
drop table if exists t2;

create table t1 (date Date, a Float64, b String) Engine=MergeTree ORDER BY date;
create table t2 (date Date, a Float64, b String) Engine=MergeTree ORDER BY date;

insert into t1(a, b) values (1, 'one'), (2, 'two');
insert into t2(a, b) values (2, 'two'), (3, 'three');

select date, a, b from t1 where (date, a, b) NOT IN (select date,a,b from t2);
select date, a, b from t2 where (date, a, b) NOT IN (select date,a,b from t1);

drop table t1;
drop table t2;
