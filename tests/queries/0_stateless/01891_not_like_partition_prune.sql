drop table if exists test;

create table test (a String)  Engine MergeTree order by a partition by a;
insert into test values('1'), ('1.1'), ('1.2'), ('1.12');

select * from test where a like '1%1';
select * from test where a not like '1%1';
select * from test where a not like '1%2';
drop table test;
