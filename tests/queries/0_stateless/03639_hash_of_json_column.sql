set session_timezone='UTC';

select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON);
select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}], "f" : null}'::JSON);
select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(max_dynamic_paths=100));
select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(max_dynamic_paths=1));
select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(max_dynamic_paths=0));
select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(max_dynamic_types=0));
select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(a Int64));
select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(a Dynamic));
select sipHash64('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(SKIP REGEXP '(abc)'));

select sipHash64('{"a" : "1970-01-01 00:00:00.000000042", "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON);

select sipHash64(tuple(map('json', [toNullable('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON)])));
select sipHash64(tuple(map('json', [toNullable('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}], "f" : null}'::JSON)])));
select sipHash64(tuple(map('json', [toNullable('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(max_dynamic_paths=100))])));
select sipHash64(tuple(map('json', [toNullable('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(max_dynamic_paths=1))])));
select sipHash64(tuple(map('json', [toNullable('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(max_dynamic_paths=0))])));
select sipHash64(tuple(map('json', [toNullable('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(max_dynamic_types=0))])));
select sipHash64(tuple(map('json', [toNullable('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(a Int64))])));
select sipHash64(tuple(map('json', [toNullable('{"a" : 42, "b" : "str", "c" : [{"d" : 1}, {"e" : 2}]}'::JSON(a Dynamic))])));


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
