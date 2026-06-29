set enable_json_type=1;

drop table if exists test;
create table test (t Tuple(a UInt32), json JSON(b UInt32), a UInt32 default t.a, b UInt32 default json.b, c UInt32 default json.c) engine=Memory;
insert into test (t, json) select tuple(42), '{"b" : 42, "c" : 42}';
select * from test;
drop table test;

create table test (t Tuple(a UInt32), json JSON(b UInt32), a UInt32 materialized t.a, b UInt32 materialized json.b, c UInt32 materialized json.c) engine=Memory;
insert into test (t, json) select tuple(42), '{"b" : 42, "c" : 42}';
select *, a, b, c from test;
drop table test;

select * from format(JSONEachRow, 't Tuple(a UInt32), json JSON(b UInt32), a UInt32 default t.a, b UInt32 default json.b, c UInt32 default json.c', '{"t" : {"a" : 42}, "json" : {"b" : 42, "c" : 42}}');

