SET enable_json_type = 1;
set enable_analyzer = 1;
set output_format_native_write_json_as_string = 0;

drop table if exists test;
create table test (json JSON(max_dynamic_paths=4, k1 UInt32, k2 String)) engine=Memory;
insert into test format JSONAsObject
{"k1" : 1, "k2" : "2020-01-01", "k3" : 31, "k4" : [1, 2, 3], "k5" : "str1",               "k7" : 71,              "k9" : 91}
{"k1" : 2, "k2" : "2020-01-02",            "k4" : [5, 6, 7],                "k6" : false,                         "k9" : 92}
{"k1" : 3, "k2" : "2020-01-03", "k3" : 32,                   "k5" : "str1",               "k7" : 73                        }
{"k1" : 4, "k2" : "2020-01-04",            "k4" : [8, 9, 0], "k5" : "str1", "k6" : true,            "k8" : 84.84, "k9" : 94}

select 'max_dynamic_paths=5';
select json::JSON(max_dynamic_paths=5, k2 DateTime, k3 UInt32, k6 Bool, k0 UInt32, SKIP k5, SKIP k8) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), json2.k0, json2.k1, json2.k2, json2.k3, json2.k4, json2.k5, json2.k6, json2.k7, json2.k8, json2.k9 from test;
select 'max_dynamic_paths=3';
select json::JSON(max_dynamic_paths=3, k2 DateTime, k3 UInt32, k6 Bool, k0 UInt32, SKIP k5, SKIP k8) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), json2.k0, json2.k1, json2.k2, json2.k3, json2.k4, json2.k5, json2.k6, json2.k7, json2.k8, json2.k9 from test;
select 'max_dynamic_paths=2';
select json::JSON(max_dynamic_paths=2, k2 DateTime, k3 UInt32, k6 Bool, k0 UInt32, SKIP k5, SKIP k8) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), json2.k0, json2.k1, json2.k2, json2.k3, json2.k4, json2.k5, json2.k6, json2.k7, json2.k8, json2.k9 from test;
select 'max_dynamic_paths=1';
select json::JSON(max_dynamic_paths=1, k2 DateTime, k3 UInt32, k6 Bool, k0 UInt32, SKIP k5, SKIP k8) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), json2.k0, json2.k1, json2.k2, json2.k3, json2.k4, json2.k5, json2.k6, json2.k7, json2.k8, json2.k9 from test;
select 'max_dynamic_paths=0';
select json::JSON(max_dynamic_paths=0, k2 DateTime, k3 UInt32, k6 Bool, k0 UInt32, SKIP k5, SKIP k8) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), json2.k0, json2.k1, json2.k2, json2.k3, json2.k4, json2.k5, json2.k6, json2.k7, json2.k8, json2.k9 from test;
select 'max_dynamic_paths=3, max_dynamic_types=0';
select json::JSON(max_dynamic_paths=3, max_dynamic_types=0, k2 DateTime, k3 UInt32, k6 Bool, k0 UInt32, SKIP k5, SKIP k8) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), isDynamicElementInSharedData(json2.k1), isDynamicElementInSharedData(json2.k9), json2.k0, json2.k1, json2.k2, json2.k3, json2.k4, json2.k5, json2.k6, json2.k7, json2.k8, json2.k9 from test;
select 'max_dynamic_paths=2, max_dynamic_types=0';
select json::JSON(max_dynamic_paths=2, max_dynamic_types=0, k2 DateTime, k3 UInt32, k6 Bool, k0 UInt32, SKIP k5, SKIP k8) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), isDynamicElementInSharedData(json2.k1), isDynamicElementInSharedData(json2.k9), json2.k0, json2.k1, json2.k2, json2.k3, json2.k4, json2.k5, json2.k6, json2.k7, json2.k8, json2.k9 from test;
select 'max_dynamic_paths=1, max_dynamic_types=0';
select json::JSON(max_dynamic_paths=1, max_dynamic_types=0, k2 DateTime, k3 UInt32, k6 Bool, k0 UInt32, SKIP k5, SKIP k8) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), isDynamicElementInSharedData(json2.k1), isDynamicElementInSharedData(json2.k9), json2.k0, json2.k1, json2.k2, json2.k3, json2.k4, json2.k5, json2.k6, json2.k7, json2.k8, json2.k9 from test;
select 'max_dynamic_paths=0, max_dynamic_types=0';
select json::JSON(max_dynamic_paths=0, max_dynamic_types=0, k2 DateTime, k3 UInt32, k6 Bool, k0 UInt32, SKIP k5, SKIP k8) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), isDynamicElementInSharedData(json2.k1), isDynamicElementInSharedData(json2.k9), json2.k0, json2.k1, json2.k2, json2.k3, json2.k4, json2.k5, json2.k6, json2.k7, json2.k8, json2.k9 from test;


drop table test;
