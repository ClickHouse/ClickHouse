set allow_experimental_json_type=1;
drop table if exists test;
create table test (json JSON) engine=Memory;
insert into test select toJSONString(map('a', 'str_' || number)) from numbers(5);
select json.a.String from test;
select json.a.:String from test;
select json.a.UInt64 from test;
drop table test;

