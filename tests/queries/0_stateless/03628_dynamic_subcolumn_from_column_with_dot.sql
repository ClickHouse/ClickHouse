drop table if exists test;
create table test (`my.json` JSON) engine=Memory;
insert into test select '{"a" : 42}';
select `my.json`.a from test;
drop table test;

