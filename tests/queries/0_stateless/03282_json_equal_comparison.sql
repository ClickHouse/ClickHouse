set enable_json_type=1;
set output_format_native_write_json_as_string=0;
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

select json1, json2, json1 == json2, json1 != json2 from test;
drop table test;
