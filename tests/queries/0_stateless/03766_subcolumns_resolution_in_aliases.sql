set enable_analyzer=1;

drop table if exists test;
create table test (a JSON, `b.c` JSON, ab UInt64 alias a.b, bcd UInt64 alias b.c.d) engine=MergeTree order by tuple();
insert into test select '{"a" : 42}', '{"d" : 43}';
select ab, bcd from test;
explain query tree select ab, bcd from test;
drop table test;

