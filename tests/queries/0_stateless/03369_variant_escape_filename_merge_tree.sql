set enable_variant_type=1;

drop table if exists test;
create table test (v Variant(Tuple(a UInt32, b UInt32))) engine=MergeTree order by tuple() settings min_rows_for_wide_part=0, min_bytes_for_wide_part=0, escape_variant_subcolumn_filenames=1, replace_long_file_name_to_hash=0;
insert into test select tuple(1, 2)::Tuple(a UInt32, b UInt32);
select filenames from system.parts_columns where table = 'test' and database = currentDatabase();
drop table test;

create table test (v Variant(Tuple(a UInt32, b UInt32))) engine=MergeTree order by tuple() settings min_rows_for_wide_part=0, min_bytes_for_wide_part=0, escape_variant_subcolumn_filenames=0, replace_long_file_name_to_hash=0;
insert into test select tuple(1, 2)::Tuple(a UInt32, b UInt32);
select filenames from system.parts_columns where table = 'test' and database = currentDatabase();
drop table test;

