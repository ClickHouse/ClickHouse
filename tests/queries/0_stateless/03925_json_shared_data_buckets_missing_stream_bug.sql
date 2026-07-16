drop table if exists src;
drop table if exists dst;
create table src (id UInt64, json JSON) engine=MergeTree order by id settings min_bytes_for_wide_part=1, object_serialization_version='v3', object_shared_data_serialization_version_for_zero_level_parts='map_with_buckets';
create table dst (id UInt64, json JSON) engine=MergeTree order by id settings min_bytes_for_wide_part=1, object_serialization_version='v3', object_shared_data_serialization_version_for_zero_level_parts='map_with_buckets';
insert into src select number, '{"a" : 42}' from numbers(10);
insert into src select number, '{"a" : 42}' from numbers(10);
insert into dst select * from src order by id desc;
select * from dst order by id;
drop table src;
drop table dst;

