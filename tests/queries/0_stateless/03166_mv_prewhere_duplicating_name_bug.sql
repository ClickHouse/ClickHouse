create table src (x Int64) engine = Log;
create table dst (s String, lc LowCardinality(String)) engine MergeTree order by s;
create materialized view mv to dst (s String, lc String) as select 'a' as s, toLowCardinality('b') as lc from src;
insert into src values (1);

select s, lc from mv where not ignore(lc) settings enable_analyzer=0;
select s, lc from mv where not ignore(lc) settings enable_analyzer=1;
