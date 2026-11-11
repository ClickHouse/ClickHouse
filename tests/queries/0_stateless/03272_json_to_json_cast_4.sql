SET enable_json_type = 1;
set enable_analyzer = 1;
set output_format_native_write_json_as_string = 0;

drop table if exists test;
create table test (json JSON(max_dynamic_paths=2, k1 UInt32, k2 String)) engine=Memory;
insert into test format JSONAsObject
{"k1" : 1, "k2" : "2020-01-01", "k3" : 31, "k4" : [1, 2, 3], "k5" : "str1"              }
{"k1" : 2, "k2" : "2020-01-02",            "k4" : [5, 6, 7],                "k6" : false}
{"k1" : 3, "k2" : "2020-01-03", "k3" : 32,                   "k5" : "str1"              }
{"k1" : 4, "k2" : "2020-01-04",            "k4" : [8, 9, 0], "k5" : "str1", "k6" : true }

select 'Add new typed path';
select json::JSON(max_dynamic_paths=2, k1 UInt32, k2 String, k3 UInt32, k6 Bool, k7 UInt32) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), json2.k1, json2.k2, json2.k3, json2.k4, json2.k5, json2.k6, json2.k7 from test;
select 'Remove typed paths';
select json::JSON(max_dynamic_paths=2) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), json2.k1, json2.k2, json2.k3, json2.k4, json2.k5, json2.k6, json2.k7 from test;
select 'Change type for typed path';
select json::JSON(max_dynamic_paths=2, k1 UInt32, k2 DateTime) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), json2.k1, json2.k2, json2.k3, json2.k4, json2.k5, json2.k6, json2.k7 from test;
select 'Remove and skip typed path';
select json::JSON(max_dynamic_paths=2, k1 UInt32, SKIP k2) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), json2.k1, json2.k2, json2.k3, json2.k4, json2.k5, json2.k6, json2.k7 from test;
select 'Remove, change and add new typed paths';
select json::JSON(max_dynamic_paths=2, k2 DateTime, k3 UInt32, k6 Bool, k7 UInt32) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), json2.k1, json2.k2, json2.k3, json2.k4, json2.k5, json2.k6, json2.k7 from test;

drop table test;
