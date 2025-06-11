set enable_json_type=1;

select '{}'::JSON as j1, '{}'::JSON as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1}'::JSON as j1, '{}'::JSON as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1}'::JSON as j1, '{"a" : 1}'::JSON as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1}'::JSON as j1, '{"a" : 2}'::JSON as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1}'::JSON as j1, '{"a" : 0}'::JSON as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1}'::JSON as j1, '{"b" : 1}'::JSON as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1}'::JSON as j1, '{"a" : 1}'::JSON as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1}'::JSON as j1, '{"a" : 1, "b" : 1}'::JSON as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1}'::JSON as j1, '{"a" : 1, "b" : 2}'::JSON as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1}'::JSON as j1, '{"a" : 1, "b" : 0}'::JSON as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1}'::JSON as j1, '{"a" : 0, "b" : 1}'::JSON as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1}'::JSON as j1, '{"a" : 2, "b" : 1}'::JSON as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1}'::JSON as j1, '{"b" : 1, "c" : 1}'::JSON as j2, j1 < j2, j1 = j2, j1 > j2;

select '{}'::JSON(a UInt32) as j1, '{}'::JSON(a UInt32) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1}'::JSON(a UInt32) as j1, '{}'::JSON(a UInt32) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1}'::JSON(a UInt32) as j1, '{"a" : 1}'::JSON(a UInt32) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1}'::JSON(a UInt32) as j1, '{"a" : 0}'::JSON(a UInt32) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1}'::JSON(a UInt32) as j1, '{"a" : 2}'::JSON(a UInt32) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1}'::JSON(a UInt32, b UInt32) as j1, '{"a" : 1}'::JSON(a UInt32, b UInt32) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1}'::JSON(a UInt32, b UInt32) as j1, '{"a" : 1, "b" : 1}'::JSON(a UInt32, b UInt32) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1}'::JSON(a UInt32, b UInt32) as j1, '{"a" : 1, "b" : 0}'::JSON(a UInt32, b UInt32) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1}'::JSON(a UInt32, b UInt32) as j1, '{"a" : 1, "b" : 2}'::JSON(a UInt32, b UInt32) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1}'::JSON(a UInt32, b UInt32) as j1, '{"a" : 0, "b" : 1}'::JSON(a UInt32, b UInt32) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1}'::JSON(a UInt32, b UInt32) as j1, '{"a" : 2, "b" : 1}'::JSON(a UInt32, b UInt32) as j2, j1 < j2, j1 = j2, j1 > j2;

select '{}'::JSON(max_dynamic_paths=2) as j1, '{}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 0, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 2, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 0, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 2, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 0, "d" : 1}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 2, "d" : 1}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 0}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 1}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 0}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 2}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"b" : 1}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"b" : 0}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"b" : 2}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 0}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 2}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 0}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 2}'::JSON(max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;

select '{}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 0, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 2, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 0, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 2, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 0, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 2, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 0, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 2, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 0, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 2, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 0}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 2}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 0}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 2}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 0}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 2}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 0}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 2}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 0}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 2}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 0}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;
select '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 1, "f" : 1}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j1, '{"a" : 1, "b" : 1, "c" : 1, "d" : 1, "e" : 2}'::JSON(a UInt32, b UInt32, max_dynamic_paths=2) as j2, j1 < j2, j1 = j2, j1 > j2;

create table test (json1 JSON(max_dynamic_paths=2, a UInt32), json2 JSON(max_dynamic_paths=2, a UInt32)) engine=Memory;
insert into test format JSONEachRow
{"json1" : {}, "json2" : {}}
{"json1" : {"a" : 42}, "json2" : {"a" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : 43}}
{"json1" : {"a" : 42, "b" : 42}, "json2" : {"a" : 42, "b" : 42}}
{"json1" : {"a" : 42, "b" : 42}, "json2" : {"a" : 42, "b" : 43}}
{"json1" : {"a" : 42, "b" : 42, "c" : 42}, "json2" : {"a" : 42, "b" : 42, "d" : 42}}
{"json1" : {"a" : 42, "b" : 42, "c" : 42, "d" : 42}, "json2" : {"a" : 42, "b" : 42, "d" : 42, "c" : 42}}
{"json1" : {"a" : 42, "b" : 42, "c" : 42, "d" : 42}, "json2" : {"a" : 42, "b" : 42, "d" : 43, "c" : 42}}
{"json1" : {"a" : 42, "b" : 42, "c" : 42, "d" : 42}, "json2" : {"a" : 42, "b" : 42, "d" : 42, "c" : 43}}
{"json1" : {"a" : 42, "b" : 42, "c" : null, "d" : 42}, "json2" : {"a" : 42, "b" : 42, "d" : 42, "c" : null}}
{"json1" : {"a" : 42, "b" : 42, "c" : null, "d" : 42}, "json2" : {"a" : 42, "b" : 42, "d" : 42, "c" : 42}}
{"json1" : {"a" : 42, "b" : 42, "c" : 42, "d" : null}, "json2" : {"a" : 42, "b" : 42, "d" : null, "c" : 42}}
{"json1" : {"a" : 42, "b" : 42, "c" : 42, "d" : null}, "json2" : {"a" : 42, "b" : 42, "d" : 42, "c" : 42}}
{"json1" : {"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42}, "json2" : {"a" : 42, "b" : 42, "d" : 42, "c" : 42, "e" : 42}}
{"json1" : {"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42}, "json2" : {"a" : 42, "b" : 42, "d" : 42, "c" : 42, "e" : "42"}}
{"json1" : {"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42}, "json2" : {"a" : 42, "b" : 42, "d" : 42, "c" : 42, "e" : 42.0}};

select json1, json2, json1 == json2, json1 != json2, json1 < json2, json1 > json2 from test;
drop table test;
