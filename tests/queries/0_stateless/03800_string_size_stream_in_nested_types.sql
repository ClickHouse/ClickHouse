drop table if exists test;
create table test (s String, ns Nullable(String), as Array(String), aas Array(Array(String)), ms Map(String, String), lcs LowCardinality(String), vs Variant(String), ds Dynamic, js JSON(s String)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, propagate_types_serialization_versions_to_nested_types=0, string_serialization_version='with_size_stream', object_serialization_version='v2';
insert into test select 'data', 'data', ['data'], [['data']], map('data', 'data'), 'data', 'data', 'data', '{"s" : "data", "ds" : "data"}';
select column, type, substreams from system.parts_columns where database=currentDatabase() and table = 'test' order by column;
drop table test;

create table test (s String, ns Nullable(String), as Array(String), aas Array(Array(String)), ms Map(String, String), lcs LowCardinality(String), vs Variant(String), ds Dynamic, js JSON(s String)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, propagate_types_serialization_versions_to_nested_types=1, string_serialization_version='with_size_stream', object_serialization_version='v2';
insert into test select 'data', 'data', ['data'], [['data']], map('data', 'data'), 'data', 'data', 'data', '{"s" : "data", "ds" : "data"}';
select column, type, substreams from system.parts_columns where database=currentDatabase() and table = 'test' order by column;
drop table test;
