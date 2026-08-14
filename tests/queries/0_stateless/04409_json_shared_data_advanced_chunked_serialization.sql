-- Tags: long, no-azure-blob-storage

set output_format_json_quote_64bit_integers=0;

drop table if exists source;
create table source (json JSON(max_dynamic_paths=8)) engine=Memory;
insert into source format JSONAsObject
{"?1" : 1, "?2" : 1, "?3" : 1, "?4" : 1, "?5" : 1, "?6" : 1, "?7" : 1, "?8" : 1, "a" : {"a1" : 1, "a2" : 2, "arr" : [{"arr1" : 3, "arr2" : 4, "arr3" : 5, "arr4" : 6}]}, "b" : 7, "c" : 8, "arr" : [{"arr1" : 9, "arr2" : 10, "arr3" : 11, "arr4" : 12}]}
{"a" : {"a1" : 2, "a2" : 3, "arr" : [{"arr1" : 4, "arr2" : 5, "arr3" : 6, "arr4" : 7}]}, "b" : 8, "c" : 9, "arr" : [{"arr1" : 10, "arr2" : 11, "arr3" : 12, "arr4" : 13}]}
{}
{}
{"a" : {"a1" : 3, "a2" : 4, "arr" : [{"arr1" : 5, "arr2" : 6, "arr3" : 7, "arr4" : 8}]}}
{"a" : {"a1" : 4, "a2" : 5, "arr" : [{"arr1" : 6, "arr2" : 7, "arr3" : 8, "arr4" : 9}]}}
{"b" : 9, "c" : 10}
{"b" : 10, "c" : 11}
{"arr" : [{"arr1" : 11, "arr2" : 12, "arr3" : 13, "arr4" : 14}]}
{"arr" : [{"arr1" : 12, "arr2" : 13, "arr3" : 14, "arr4" :15 }]}
{"a" : {"a1" : 5, "a2" : 6}}
{"a" : {"a1" : 6, "a2" : 7}};

-- Test compact parts with advanced_chunked serialization.
-- 12 rows with target_chunk_rows=4 produces 3 chunks.
drop table if exists test_compact_chunked;
create table test_compact_chunked (json JSON(max_dynamic_paths=8)) engine=MergeTree order by tuple() settings index_granularity=2, min_bytes_for_wide_part='200G', min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_serialization_version='advanced_chunked', object_shared_data_serialization_version_for_zero_level_parts='advanced_chunked', object_shared_data_buckets_for_compact_part=2, object_shared_data_target_chunk_rows=4;
insert into test_compact_chunked select * from source;

select 'compact chunked: select json';
select json from test_compact_chunked;
select 'compact chunked: select json.b';
select json.b from test_compact_chunked;
select 'compact chunked: select json.b, json.c';
select json.b, json.c from test_compact_chunked;
select 'compact chunked: select json.arr[].arr1';
select json.arr[].arr1 from test_compact_chunked;
select 'compact chunked: select json.arr[].arr1, json.arr[].arr2';
select json.arr[].arr1, json.arr[].arr2 from test_compact_chunked;
select 'compact chunked: select json, json.b, json.c';
select json, json.b, json.c from test_compact_chunked;
select 'compact chunked: select json.^a';
select json.^a from test_compact_chunked;
select 'compact chunked: select json.^a, json.a.a1, json.a.arr[].arr1';
select json.^a, json.a.a1, json.a.arr[].arr1 from test_compact_chunked;
select 'compact chunked: select json, json.^a, json.a.a1, json.a.arr[].arr1, json.b, json.arr';
select json, json.^a, json.a.a1, json.a.arr[].arr1, json.b, json.arr from test_compact_chunked;

-- Test merge with compact parts.
drop table if exists test_compact_chunked_merge;
create table test_compact_chunked_merge (json JSON(max_dynamic_paths=8)) engine=MergeTree order by tuple() settings index_granularity=2, min_bytes_for_wide_part='200G', min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_serialization_version='advanced_chunked', object_shared_data_serialization_version_for_zero_level_parts='advanced_chunked', object_shared_data_buckets_for_compact_part=2, object_shared_data_target_chunk_rows=4;
insert into test_compact_chunked_merge select * from source;
insert into test_compact_chunked_merge select * from source;
optimize table test_compact_chunked_merge final;

select 'compact chunked merge: select json';
select json from test_compact_chunked_merge order by json::String;
select 'compact chunked merge: select json, json.^a, json.a.a1, json.a.arr[].arr1, json.b, json.arr';
select json, json.^a, json.a.a1, json.a.arr[].arr1, json.b, json.arr from test_compact_chunked_merge order by json::String;

drop table test_compact_chunked;
drop table test_compact_chunked_merge;

-- Test wide parts with advanced_chunked serialization.
drop table if exists test_wide_chunked;
create table test_wide_chunked (json JSON(max_dynamic_paths=8)) engine=MergeTree order by tuple() settings index_granularity=2, min_bytes_for_wide_part=1, min_rows_for_wide_part=1, object_serialization_version='v3', object_shared_data_serialization_version='advanced_chunked', object_shared_data_serialization_version_for_zero_level_parts='advanced_chunked', object_shared_data_buckets_for_wide_part=2, object_shared_data_target_chunk_rows=4;
insert into test_wide_chunked select * from source;

select 'wide chunked: select json';
select json from test_wide_chunked;
select 'wide chunked: select json.b';
select json.b from test_wide_chunked;
select 'wide chunked: select json.b, json.c';
select json.b, json.c from test_wide_chunked;
select 'wide chunked: select json.arr[].arr1';
select json.arr[].arr1 from test_wide_chunked;
select 'wide chunked: select json.arr[].arr1, json.arr[].arr2';
select json.arr[].arr1, json.arr[].arr2 from test_wide_chunked;
select 'wide chunked: select json, json.b, json.c';
select json, json.b, json.c from test_wide_chunked;
select 'wide chunked: select json.^a';
select json.^a from test_wide_chunked;
select 'wide chunked: select json.^a, json.a.a1, json.a.arr[].arr1';
select json.^a, json.a.a1, json.a.arr[].arr1 from test_wide_chunked;
select 'wide chunked: select json, json.^a, json.a.a1, json.a.arr[].arr1, json.b, json.arr';
select json, json.^a, json.a.a1, json.a.arr[].arr1, json.b, json.arr from test_wide_chunked;

-- Test merge with wide parts.
drop table if exists test_wide_chunked_merge;
create table test_wide_chunked_merge (json JSON(max_dynamic_paths=8)) engine=MergeTree order by tuple() settings index_granularity=2, min_bytes_for_wide_part=1, min_rows_for_wide_part=1, object_serialization_version='v3', object_shared_data_serialization_version='advanced_chunked', object_shared_data_serialization_version_for_zero_level_parts='advanced_chunked', object_shared_data_buckets_for_wide_part=2, object_shared_data_target_chunk_rows=4;
insert into test_wide_chunked_merge select * from source;
insert into test_wide_chunked_merge select * from source;
optimize table test_wide_chunked_merge final;

select 'wide chunked merge: select json';
select json from test_wide_chunked_merge order by json::String;
select 'wide chunked merge: select json, json.^a, json.a.a1, json.a.arr[].arr1, json.b, json.arr';
select json, json.^a, json.a.a1, json.a.arr[].arr1, json.b, json.arr from test_wide_chunked_merge order by json::String;

drop table test_wide_chunked;
drop table test_wide_chunked_merge;

-- Test Array(JSON) which is the main use case for chunk splitting.
drop table if exists test_array_json_chunked;
create table test_array_json_chunked (id UInt64, data Array(JSON(max_dynamic_paths=2))) engine=MergeTree order by id settings index_granularity=4, min_bytes_for_wide_part=1, min_rows_for_wide_part=1, object_serialization_version='v3', object_shared_data_serialization_version='advanced_chunked', object_shared_data_serialization_version_for_zero_level_parts='advanced_chunked', object_shared_data_buckets_for_wide_part=2, object_shared_data_target_chunk_rows=4;
insert into test_array_json_chunked values (1, ['{"a":1,"b":2,"c":3}', '{"a":4,"d":5,"e":6}']),(2, ['{"a":7,"f":8}']),(3, ['{"g":9,"h":10,"a":11}', '{"a":12,"b":13}', '{"i":14}']),(4, []);
insert into test_array_json_chunked values (5, ['{"a":15,"j":16}']),(6, ['{"k":17,"l":18}', '{"a":19}']),(7, []),(8, ['{"a":20,"m":21,"n":22}']);

select 'array json chunked: select id, data';
select id, data from test_array_json_chunked order by id;
select 'array json chunked: select id, data.a';
select id, data.a from test_array_json_chunked order by id;

optimize table test_array_json_chunked final;

select 'array json chunked after merge: select id, data';
select id, data from test_array_json_chunked order by id;
select 'array json chunked after merge: select id, data.a';
select id, data.a from test_array_json_chunked order by id;

drop table test_array_json_chunked;

-- Test compact Array(JSON) with chunking.
drop table if exists test_array_json_compact_chunked;
create table test_array_json_compact_chunked (id UInt64, data Array(JSON(max_dynamic_paths=2))) engine=MergeTree order by id settings index_granularity=4, min_bytes_for_wide_part='200G', min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_serialization_version='advanced_chunked', object_shared_data_serialization_version_for_zero_level_parts='advanced_chunked', object_shared_data_buckets_for_compact_part=2, object_shared_data_target_chunk_rows=4;
insert into test_array_json_compact_chunked values (1, ['{"a":1,"b":2,"c":3}', '{"a":4,"d":5,"e":6}']),(2, ['{"a":7,"f":8}']),(3, ['{"g":9,"h":10,"a":11}', '{"a":12,"b":13}', '{"i":14}']),(4, []);
insert into test_array_json_compact_chunked values (5, ['{"a":15,"j":16}']),(6, ['{"k":17,"l":18}', '{"a":19}']),(7, []),(8, ['{"a":20,"m":21,"n":22}']);

select 'array json compact chunked: select id, data';
select id, data from test_array_json_compact_chunked order by id;
select 'array json compact chunked: select id, data.a';
select id, data.a from test_array_json_compact_chunked order by id;

optimize table test_array_json_compact_chunked final;

select 'array json compact chunked after merge: select id, data';
select id, data from test_array_json_compact_chunked order by id;
select 'array json compact chunked after merge: select id, data.a';
select id, data.a from test_array_json_compact_chunked order by id;

drop table test_array_json_compact_chunked;

drop table source;
