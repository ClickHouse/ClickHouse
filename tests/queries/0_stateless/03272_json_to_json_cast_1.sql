SET enable_json_type = 1;
set enable_analyzer = 1;
set output_format_native_write_json_as_string=0;

drop table if exists test;
create table test (json JSON(max_dynamic_paths=2, max_dynamic_types=2, a UInt32, b String, SKIP c)) engine=Memory;
insert into test format JSONAsObject
{"a" : 1, "b" : "str1", "k1" : 1, "k2" : 2, "k3" : 3, "k4" : 4},
{"a" : 2, "b" : "str2", "c" : 42, "k1" : "kstr1", "k2" : "kstr2", "k3" : "kstr3", "k4" : "kstr4"},
{"a" : 3, "b" : "str3", "k1" : [1], "k2" : [2], "k3" : [3], "k4" : [4]},
{"a" : 4, "b" : "str4", "c" : 42, "k1" : 5, "k2" : 6, "k3" : 7, "k4" : 8};

select 'Keep max_dynamic_paths, decrease max_dynamic_types';
select
    json::JSON(max_dynamic_paths=2, max_dynamic_types=1, a UInt32, b String, SKIP c) as json2,
    JSONDynamicPaths(json2),
    JSONSharedDataPaths(json2),
    dynamicType(json2.k2),
    isDynamicElementInSharedData(json2.k2),
    dynamicType(json2.k4),
    isDynamicElementInSharedData(json2.k4)
from test;

select 'Keep max_dynamic_paths, increase max_dynamic_types';
select
    json::JSON(max_dynamic_paths=2, max_dynamic_types=4, a UInt32, b String, SKIP c) as json2,
    JSONDynamicPaths(json2),
    JSONSharedDataPaths(json2),
    dynamicType(json2.k2),
    isDynamicElementInSharedData(json2.k2),
    dynamicType(json2.k4),
    isDynamicElementInSharedData(json2.k4)
from test;

select 'Increase max_dynamic_paths, keep max_dynamic_types';
select
    json::JSON(max_dynamic_paths=4, max_dynamic_types=2, a UInt32, b String, SKIP c) as json2,
    JSONDynamicPaths(json2),
    JSONSharedDataPaths(json2),
    dynamicType(json2.k2),
    isDynamicElementInSharedData(json2.k2),
    dynamicType(json2.k4),
    isDynamicElementInSharedData(json2.k4)
from test;

select 'Increase max_dynamic_paths, decrease max_dynamic_types';
select
    json::JSON(max_dynamic_paths=4, max_dynamic_types=1, a UInt32, b String, SKIP c) as json2,
    JSONDynamicPaths(json2),
    JSONSharedDataPaths(json2),
    dynamicType(json2.k2),
    isDynamicElementInSharedData(json2.k2),
    dynamicType(json2.k4),
    isDynamicElementInSharedData(json2.k4)
from test;

select 'Increase max_dynamic_paths, increase max_dynamic_types';
select
    json::JSON(max_dynamic_paths=4, max_dynamic_types=4, a UInt32, b String, SKIP c) as json2,
    JSONDynamicPaths(json2),
    JSONSharedDataPaths(json2),
    dynamicType(json2.k2),
    isDynamicElementInSharedData(json2.k2),
    dynamicType(json2.k4),
    isDynamicElementInSharedData(json2.k4)
from test;

select 'Decrease max_dynamic_paths, keep max_dynamic_types';
select
    json::JSON(max_dynamic_paths=1, max_dynamic_types=2, a UInt32, b String, SKIP c) as json2,
    JSONDynamicPaths(json2),
    JSONSharedDataPaths(json2),
    dynamicType(json2.k2),
    isDynamicElementInSharedData(json2.k2),
    dynamicType(json2.k4),
    isDynamicElementInSharedData(json2.k4)
from test;

select 'Decrease max_dynamic_paths, decrease max_dynamic_types';
select
    json::JSON(max_dynamic_paths=1, max_dynamic_types=1, a UInt32, b String, SKIP c) as json2,
    JSONDynamicPaths(json2),
    JSONSharedDataPaths(json2),
    dynamicType(json2.k2),
    isDynamicElementInSharedData(json2.k2),
    dynamicType(json2.k4),
    isDynamicElementInSharedData(json2.k4)
from test;

select 'Decrease max_dynamic_paths, increase max_dynamic_types';
select
    json::JSON(max_dynamic_paths=1, max_dynamic_types=4, a UInt32, b String, SKIP c) as json2,
    JSONDynamicPaths(json2),
    JSONSharedDataPaths(json2),
    dynamicType(json2.k2),
    isDynamicElementInSharedData(json2.k2),
    dynamicType(json2.k4),
    isDynamicElementInSharedData(json2.k4)
from test;
