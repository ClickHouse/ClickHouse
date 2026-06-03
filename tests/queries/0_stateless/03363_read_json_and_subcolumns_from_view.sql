set enable_json_type=1;
set enable_analyzer=1;

drop table if exists test;
create table test (data JSON) engine=Memory;
insert into test select '{"a" : 42}';
create view test_view as select data from test;
select * from test_view;
select data from test_view;
select data.a from test_view;
select data.b from test_view;
select data.a.:Int64 from test_view;
drop table test;

