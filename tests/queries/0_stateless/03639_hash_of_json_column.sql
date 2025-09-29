select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON);
select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}], "f" : null}'::JSON);

select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(max_dynamic_paths=1));
select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}], "f" : null}'::JSON(max_dynamic_paths=1));

select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(max_dynamic_types=0));
select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}], "f" : null}'::JSON(max_dynamic_types=0));

select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(a UInt32));
select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}], "f" : null}'::JSON(a UInt32));

select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(a UInt32, max_dynamic_paths=1));
select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}], "f" : null}'::JSON(a UInt32, max_dynamic_paths=1));

select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(a UInt32, max_dynamic_types=0));
select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}], "f" : null}'::JSON(a UInt32, max_dynamic_types=0));

drop table if exists test;
create table test (json JSON) engine=Memory;
insert into test values ('{"a" : 1}'), ('{"a" : 2}'), ('{}'), ('{"a" : null}'), ('{"b" : 1}');
select json, sipHash64(json) from test;
drop table test;

create table test (json JSON(max_dynamic_types=0)) engine=Memory;
insert into test values ('{"a" : 1}'), ('{"a" : 2}'), ('{}'), ('{"a" : null}'), ('{"b" : 1}');
select json, sipHash64(json) from test;
drop table test;

create table test (json JSON(max_dynamic_paths=0)) engine=Memory;
insert into test values ('{"a" : 1}'), ('{"a" : 2}'), ('{}'), ('{"a" : null}'), ('{"b" : 1}');
select json, sipHash64(json) from test;
drop table test;
