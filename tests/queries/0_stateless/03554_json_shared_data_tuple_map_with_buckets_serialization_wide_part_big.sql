-- Tags: long
-- Random settings limits: index_granularity=(100, None); index_granularity_bytes=(100000, None)

drop table if exists test_wide_map_with_buckets_tuple;
create table test_wide_map_with_buckets_tuple (json Tuple(data JSON(max_dynamic_paths=8))) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_serialization_version='map_with_buckets', object_shared_data_serialization_version_for_zero_level_parts='map_with_buckets', object_shared_data_buckets_for_wide_part=2;
insert into test_wide_map_with_buckets_tuple select tuple(multiIf(
number < 15000,
'{"?1" : 1, "?2" : 1, "?3" : 1, "?4" : 1, "?5" : 1, "?6" : 1, "?7" : 1, "?8" : 1}',
number < 20000,
'{"a" : {"a1" : 1, "a2" : 2, "arr" : [{"arr1" : 3, "arr2" : 4, "arr3" : 5, "arr4" : 6}]}, "b" : 7, "c" : 8, "arr" : [{"arr1" : 9, "arr2" : 10, "arr3" : 11, "arr4" : 12}]}',
number < 25000,
'{}',
number < 30000,
'{"a" : {"a1" : 3, "a2" : 4, "arr" : [{"arr1" : 5, "arr2" : 6, "arr3" : 7, "arr4" : 8}]}}',
number < 35000,
'{"b" : 9, "c" : 10}',
number < 40000,
'{"arr" : [{"arr1" : 11, "arr2" : 12, "arr3" : 13, "arr4" : 14}]}',
'{"a" : {"a1" : 5, "a2" : 6}}'
)) from numbers(45000);

select 'select json.data';
select json.data from test_wide_map_with_buckets_tuple format Null;
select 'select json.data, json.data.b';
select json.data, json.data.b from test_wide_map_with_buckets_tuple format Null;
select 'select json.data.b, json.data';
select json.data.b, json.data from test_wide_map_with_buckets_tuple format Null;
select 'select json.data, json.data.b, json.data.c';
select json.data, json.data.b, json.data.c from test_wide_map_with_buckets_tuple format Null;
select 'select json.data.b, json.data, json.data.c';
select json.data.b, json.data, json.data.c from test_wide_map_with_buckets_tuple format Null;
select 'select json.data.b, json.data.c, json.data';
select json.data.b, json.data.c, json.data from test_wide_map_with_buckets_tuple format Null;
select 'select json.data, json.data.^a';
select json.data, json.data.^a from test_wide_map_with_buckets_tuple format Null;
select 'select json.data.^a, json.data';
select json.data.^a, json.data from test_wide_map_with_buckets_tuple format Null;
select 'select json.data, json.data.^a, json.data.b';
select json.data, json.data.^a, json.data.b from test_wide_map_with_buckets_tuple format Null;
select 'select json.data.b, json.data.^a, json.data';
select json.data.b, json.data.^a, json.data from test_wide_map_with_buckets_tuple format Null;

drop table test_wide_map_with_buckets_tuple;
