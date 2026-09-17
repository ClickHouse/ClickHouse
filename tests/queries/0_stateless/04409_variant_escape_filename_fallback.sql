-- Test bidirectional fallback for escape_variant_subcolumn_filenames setting.
-- When replicas in a hydra group have different values of this setting,
-- parts written with escaping enabled must be readable by replicas with escaping disabled and vice versa.

set enable_variant_type=1;

-- Case 1: Start with escaping disabled, then switch to enabled.
-- Parts written without escaping should still be readable after enabling escaping.
drop table if exists test_fallback;
create table test_fallback (v Variant(Tuple(a UInt32, b UInt32))) engine=MergeTree order by tuple() settings min_rows_for_wide_part=0, min_bytes_for_wide_part=0, escape_variant_subcolumn_filenames=0, replace_long_file_name_to_hash=0;
insert into test_fallback select tuple(1, 2)::Tuple(a UInt32, b UInt32);
select v, v.`Tuple(a UInt32, b UInt32)`.a, v.`Tuple(a UInt32, b UInt32)`.b from test_fallback;

alter table test_fallback modify setting escape_variant_subcolumn_filenames=1;
-- Old part (unescaped filenames) must still be readable with the new setting.
select v, v.`Tuple(a UInt32, b UInt32)`.a, v.`Tuple(a UInt32, b UInt32)`.b from test_fallback;
-- Insert new data with escaping enabled.
insert into test_fallback select tuple(3, 4)::Tuple(a UInt32, b UInt32);
select v, v.`Tuple(a UInt32, b UInt32)`.a, v.`Tuple(a UInt32, b UInt32)`.b from test_fallback order by v.`Tuple(a UInt32, b UInt32)`.a;
drop table test_fallback;

-- Case 2: Start with escaping enabled, then switch to disabled.
-- Parts written with escaping should still be readable after disabling escaping.
drop table if exists test_fallback;
create table test_fallback (v Variant(Tuple(a UInt32, b UInt32))) engine=MergeTree order by tuple() settings min_rows_for_wide_part=0, min_bytes_for_wide_part=0, escape_variant_subcolumn_filenames=1, replace_long_file_name_to_hash=0;
insert into test_fallback select tuple(5, 6)::Tuple(a UInt32, b UInt32);
select v, v.`Tuple(a UInt32, b UInt32)`.a, v.`Tuple(a UInt32, b UInt32)`.b from test_fallback;

alter table test_fallback modify setting escape_variant_subcolumn_filenames=0;
-- Old part (escaped filenames) must still be readable with the new setting.
select v, v.`Tuple(a UInt32, b UInt32)`.a, v.`Tuple(a UInt32, b UInt32)`.b from test_fallback;
-- Insert new data with escaping disabled.
insert into test_fallback select tuple(7, 8)::Tuple(a UInt32, b UInt32);
select v, v.`Tuple(a UInt32, b UInt32)`.a, v.`Tuple(a UInt32, b UInt32)`.b from test_fallback order by v.`Tuple(a UInt32, b UInt32)`.a;
drop table test_fallback;

-- Case 3: RENAME COLUMN after switching escaping from disabled to enabled.
-- Parts written without escaping must survive a column rename after enabling escaping.
drop table if exists test_fallback;
create table test_fallback (v Variant(Tuple(a UInt32, b UInt32))) engine=MergeTree order by tuple() settings min_rows_for_wide_part=0, min_bytes_for_wide_part=0, escape_variant_subcolumn_filenames=0, replace_long_file_name_to_hash=0;
insert into test_fallback select tuple(1, 2)::Tuple(a UInt32, b UInt32);
alter table test_fallback modify setting escape_variant_subcolumn_filenames=1;
alter table test_fallback rename column v to w;
select w, w.`Tuple(a UInt32, b UInt32)`.a, w.`Tuple(a UInt32, b UInt32)`.b from test_fallback;
drop table test_fallback;

-- Case 4: RENAME COLUMN after switching escaping from enabled to disabled.
-- Parts written with escaping must survive a column rename after disabling escaping.
drop table if exists test_fallback;
create table test_fallback (v Variant(Tuple(a UInt32, b UInt32))) engine=MergeTree order by tuple() settings min_rows_for_wide_part=0, min_bytes_for_wide_part=0, escape_variant_subcolumn_filenames=1, replace_long_file_name_to_hash=0;
insert into test_fallback select tuple(3, 4)::Tuple(a UInt32, b UInt32);
alter table test_fallback modify setting escape_variant_subcolumn_filenames=0;
alter table test_fallback rename column v to w;
select w, w.`Tuple(a UInt32, b UInt32)`.a, w.`Tuple(a UInt32, b UInt32)`.b from test_fallback;
drop table test_fallback;
