drop table if exists test;
create table test (json JSON(max_dynamic_paths=4)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, merge_max_dynamic_subcolumns_in_wide_part=2;
insert into test select '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}';
select JSONDynamicPaths(json), JSONSharedDataPaths(json) from test;
insert into test select '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}';
optimize table test final;
select JSONDynamicPaths(json), JSONSharedDataPaths(json) from test limit 1;
drop table test;

create table test (x UInt32, json JSON(max_dynamic_paths=4)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, merge_max_dynamic_subcolumns_in_wide_part=2, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1;
insert into test select 42,  '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}';
select JSONDynamicPaths(json), JSONSharedDataPaths(json) from test;
insert into test select 42, '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}';
optimize table test final;
select JSONDynamicPaths(json), JSONSharedDataPaths(json) from test limit 1;
drop table test