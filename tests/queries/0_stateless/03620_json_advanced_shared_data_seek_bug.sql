set use_variant_as_common_type = 1;

drop table if exists test;
create table test (id UInt64, json JSON(max_dynamic_paths=2, a.b.c UInt32)) engine=MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000, index_granularity = 39547, object_serialization_version = 'v3', object_shared_data_serialization_version = 'advanced', object_shared_data_serialization_version_for_zero_level_parts = 'advanced', object_shared_data_buckets_for_compact_part = 3, object_shared_data_buckets_for_wide_part = 1, dynamic_serialization_version = 'v3';

insert into test select number, number < 500000 ? toJSONString(map('a.b.c', number, 'a.b.d', number::UInt32, 'a.b.e', 'str_' || toString(number))) :  toJSONString(map('a.b.c', number, 'a.b.d', number::UInt32, 'a.b.e', 'str_' || toString(number), 'b.b._' || toString(number % 5), number::UInt32)) from numbers(400000, 200000);

select json.b.b.`_1`.:String from test format Null settings max_threads=1;
drop table test;
