drop table if exists test;
create table test (id UInt64, json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='100G', write_marks_for_substreams_in_compact_parts=0;
insert into test select number, '{"a" : 42, "b" : {"c" : 42}}' from numbers(100000);
select json.^b from test order by id format Null;
drop table test;
