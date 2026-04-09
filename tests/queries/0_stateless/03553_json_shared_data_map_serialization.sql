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

drop table if exists test_compact_map;
create table test_compact_map (json JSON(max_dynamic_paths=8)) engine=MergeTree order by tuple() settings index_granularity=2, min_bytes_for_wide_part='200G', min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_serialization_version='map', object_shared_data_serialization_version_for_zero_level_parts='map';
insert into test_compact_map select * from source;

select 'select json';
select json from test_compact_map;
select 'select json.b';
select json.b from test_compact_map;
select 'select json.b, json.c';
select json.b, json.c from test_compact_map;
select 'select json, json.b, json.c';
select json, json.b, json.c from test_compact_map;
select 'select json.b, json.c, json';
select json.b, json.c, json from test_compact_map;
select 'select json.^a';
select json.^a from test_compact_map;
select 'select json, json.^a';
select json, json.^a from test_compact_map;
select 'select json.^a, json.a.a1';
select json.^a, json.a.a1 from test_compact_map;
select 'select json.a.a1, json.^a';
select json.a.a1, json.^a from test_compact_map;
drop table test_compact_map;

drop table if exists test_compact_map_tuple;
create table test_compact_map_tuple (json Tuple(data JSON(max_dynamic_paths=8))) engine=MergeTree order by tuple() settings index_granularity=2, min_bytes_for_wide_part='200G', min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_serialization_version='map', object_shared_data_serialization_version_for_zero_level_parts='map';
insert into test_compact_map_tuple select tuple(json) from source;

select 'select json.data';
select json.data from test_compact_map_tuple;
select 'select json.data, json.data.b';
select json.data, json.data.b from test_compact_map_tuple;
select 'select json.data.b, json.data';
select json.data.b, json.data from test_compact_map_tuple;
select 'select json.data, json.data.b, json.data.c';
select json.data, json.data.b, json.data.c from test_compact_map_tuple;
select 'select json.data.b, json.data, json.data.c';
select json.data.b, json.data, json.data.c from test_compact_map_tuple;
select 'select json.data.b, json.data.c, json.data';
select json.data.b, json.data.c, json.data from test_compact_map_tuple;
select 'select json.data, json.data.^a';
select json.data, json.data.^a from test_compact_map_tuple;
select 'select json.data.^a, json.data';
select json.data.^a, json.data from test_compact_map_tuple;
select 'select json.data, json.data.^a, json.data.b';
select json.data, json.data.^a, json.data.b from test_compact_map_tuple;
select 'select json.data.b, json.data.^a, json.data';
select json.data.b, json.data.^a, json.data from test_compact_map_tuple;

drop table test_compact_map_tuple;

drop table if exists test_wide_map;
create table test_wide_map (json JSON(max_dynamic_paths=8)) engine=MergeTree order by tuple() settings index_granularity=2, min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_serialization_version='map', object_shared_data_serialization_version_for_zero_level_parts='map';
insert into test_wide_map select * from source;

select 'select json';
select json from test_wide_map;
select 'select json.b';
select json.b from test_wide_map;
select 'select json.b, json.c';
select json.b, json.c from test_wide_map;
select 'select json, json.b, json.c';
select json, json.b, json.c from test_wide_map;
select 'select json.b, json.c, json';
select json.b, json.c, json from test_wide_map;
select 'select json.^a';
select json.^a from test_wide_map;
select 'select json, json.^a';
select json, json.^a from test_wide_map;
select 'select json.^a, json.a.a1';
select json.^a, json.a.a1 from test_wide_map;
select 'select json.a.a1, json.^a';
select json.a.a1, json.^a from test_wide_map;

select 'select json limit 3';
select json from test_wide_map limit 3;
select 'select json.b limit 3';
select json.b from test_wide_map limit 3;
select 'select json.b, json.c limit 3';
select json.b, json.c from test_wide_map limit 3;
select 'select json, json.b, json.c limit 3';
select json, json.b, json.c from test_wide_map limit 3;
select 'select json.b, json.c, json limit 3';
select json.b, json.c, json from test_wide_map limit 3;
select 'select json.^a limit 3';
select json.^a from test_wide_map limit 3;
select 'select json, json.^a limit 3';
select json, json.^a from test_wide_map limit 3;
select 'select json.^a, json.a.a1 limit 3';
select json.^a, json.a.a1 from test_wide_map limit 3;
select 'select json.a.a1, json.^a limit 3';
select json.a.a1, json.^a from test_wide_map limit 3;

select 'select json settings max_block_size=3';
select json from test_wide_map settings max_block_size=3;
select 'select json.b settings max_block_size=3';
select json.b from test_wide_map settings max_block_size=3;
select 'select json.b, json.c settings max_block_size=3';
select json.b, json.c from test_wide_map settings max_block_size=3;
select 'select json, json.b, json.c settings max_block_size=3';
select json, json.b, json.c from test_wide_map settings max_block_size=3;
select 'select json.b, json.c, json settings max_block_size=3';
select json.b, json.c, json from test_wide_map settings max_block_size=3;
select 'select json.^a settings max_block_size=3';
select json.^a from test_wide_map settings max_block_size=3;
select 'select json, json.^a settings max_block_size=3';
select json, json.^a from test_wide_map settings max_block_size=3;
select 'select json.^a, json.a.a1 settings max_block_size=3';
select json.^a, json.a.a1 from test_wide_map settings max_block_size=3;
select 'select json.a.a1, json.^a settings max_block_size=3';
select json.a.a1, json.^a from test_wide_map settings max_block_size=3;


drop table test_wide_map;

drop table if exists test_wide_map_tuple;
create table test_wide_map_tuple (json Tuple(data JSON(max_dynamic_paths=8))) engine=MergeTree order by tuple() settings index_granularity=2, min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_serialization_version='map', object_shared_data_serialization_version_for_zero_level_parts='map';
insert into test_wide_map_tuple select tuple(json) from source;

select 'select json.data';
select json.data from test_wide_map_tuple;
select 'select json.data, json.data.b';
select json.data, json.data.b from test_wide_map_tuple;
select 'select json.data.b, json.data';
select json.data.b, json.data from test_wide_map_tuple;
select 'select json.data, json.data.b, json.data.c';
select json.data, json.data.b, json.data.c from test_wide_map_tuple;
select 'select json.data.b, json.data, json.data.c';
select json.data.b, json.data, json.data.c from test_wide_map_tuple;
select 'select json.data.b, json.data.c, json.data';
select json.data.b, json.data.c, json.data from test_wide_map_tuple;
select 'select json.data, json.data.^a';
select json.data, json.data.^a from test_wide_map_tuple;
select 'select json.data.^a, json.data';
select json.data.^a, json.data from test_wide_map_tuple;
select 'select json.data, json.data.^a, json.data.b';
select json.data, json.data.^a, json.data.b from test_wide_map_tuple;
select 'select json.data.b, json.data.^a, json.data';
select json.data.b, json.data.^a, json.data from test_wide_map_tuple;

select 'select json.data limit 3';
select json.data from test_wide_map_tuple limit 3;
select 'select json.data, json.data.b limit 3';
select json.data, json.data.b from test_wide_map_tuple limit 3;
select 'select json.data.b, json.data limit 3';
select json.data.b, json.data from test_wide_map_tuple limit 3;
select 'select json.data, json.data.b, json.data.c limit 3';
select json.data, json.data.b, json.data.c from test_wide_map_tuple limit 3;
select 'select json.data.b, json.data, json.data.c limit 3';
select json.data.b, json.data, json.data.c from test_wide_map_tuple limit 3;
select 'select json.data.b, json.data.c, json.data limit 3';
select json.data.b, json.data.c, json.data from test_wide_map_tuple limit 3;
select 'select json.data, json.data.^a limit 3';
select json.data, json.data.^a from test_wide_map_tuple limit 3;
select 'select json.data.^a, json.data limit 3';
select json.data.^a, json.data from test_wide_map_tuple limit 3;
select 'select json.data, json.data.^a, json.data.b limit 3';
select json.data, json.data.^a, json.data.b from test_wide_map_tuple limit 3;
select 'select json.data.b, json.data.^a, json.data limit 3';
select json.data.b, json.data.^a, json.data from test_wide_map_tuple limit 3;

select 'select json.data settings max_block_size=3';
select json.data from test_wide_map_tuple settings max_block_size=3;
select 'select json.data, json.data.b settings max_block_size=3';
select json.data, json.data.b from test_wide_map_tuple settings max_block_size=3;
select 'select json.data.b, json.data settings max_block_size=3';
select json.data.b, json.data from test_wide_map_tuple settings max_block_size=3;
select 'select json.data, json.data.b, json.data.c settings max_block_size=3';
select json.data, json.data.b, json.data.c from test_wide_map_tuple settings max_block_size=3;
select 'select json.data.b, json.data, json.data.c settings max_block_size=3';
select json.data.b, json.data, json.data.c from test_wide_map_tuple settings max_block_size=3;
select 'select json.data.b, json.data.c, json.data settings max_block_size=3';
select json.data.b, json.data.c, json.data from test_wide_map_tuple settings max_block_size=3;
select 'select json.data, json.data.^a settings max_block_size=3';
select json.data, json.data.^a from test_wide_map_tuple settings max_block_size=3;
select 'select json.data.^a, json.data settings max_block_size=3';
select json.data.^a, json.data from test_wide_map_tuple settings max_block_size=3;
select 'select json.data, json.data.^a, json.data.b settings max_block_size=3';
select json.data, json.data.^a, json.data.b from test_wide_map_tuple settings max_block_size=3;
select 'select json.data.b, json.data.^a, json.data settings max_block_size=3';
select json.data.b, json.data.^a, json.data from test_wide_map_tuple settings max_block_size=3;

drop table test_wide_map_tuple;
drop table source;
