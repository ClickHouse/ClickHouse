drop table if exists test ;
create table test(str Nullable(String), i Int64) engine=Memory();
insert into test values(null, 1),('', 2),('s', 1);
select '-----------String------------';
select str, max(i) from test group by str order by str nulls first;

drop table test;
