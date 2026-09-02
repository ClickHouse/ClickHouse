-- Compact-part regression for the bidirectional escape_variant_subcolumn_filenames fallback.
-- A bucketed Map inside a Variant has an optional MapBucketIndexes substream that preserves the
-- original key order. In compact parts its existence is probed against columns_substreams.txt, whose
-- names use the write-time setting; after the setting is flipped the probe used to miss it and take
-- the "no bucket index" path, reordering the map elements. with_buckets serialization and a constant
-- multi-bucket layout are forced so the substream is actually produced.

set enable_variant_type=1;

-- Case 1: written with escaping disabled, then enabled.
drop table if exists test_escape_compact;
create table test_escape_compact (v Variant(Map(String, UInt32))) engine=MergeTree order by tuple()
    settings escape_variant_subcolumn_filenames=0, replace_long_file_name_to_hash=0,
             map_serialization_version='with_buckets', map_serialization_version_for_zero_level_parts='with_buckets',
             map_buckets_strategy='constant', max_buckets_in_map=4, map_buckets_min_avg_size=0;
insert into test_escape_compact select map('a', 1, 'b', 2, 'c', 3, 'd', 4, 'e', 5)::Map(String, UInt32);
alter table test_escape_compact modify setting escape_variant_subcolumn_filenames=1;
select v from test_escape_compact;
select v.`Map(String, UInt32)`.keys from test_escape_compact;
select v.`Map(String, UInt32)`.values from test_escape_compact;
drop table test_escape_compact;

-- Case 2: written with escaping enabled, then disabled.
drop table if exists test_escape_compact;
create table test_escape_compact (v Variant(Map(String, UInt32))) engine=MergeTree order by tuple()
    settings escape_variant_subcolumn_filenames=1, replace_long_file_name_to_hash=0,
             map_serialization_version='with_buckets', map_serialization_version_for_zero_level_parts='with_buckets',
             map_buckets_strategy='constant', max_buckets_in_map=4, map_buckets_min_avg_size=0;
insert into test_escape_compact select map('a', 1, 'b', 2, 'c', 3, 'd', 4, 'e', 5)::Map(String, UInt32);
alter table test_escape_compact modify setting escape_variant_subcolumn_filenames=0;
select v from test_escape_compact;
select v.`Map(String, UInt32)`.keys from test_escape_compact;
select v.`Map(String, UInt32)`.values from test_escape_compact;
drop table test_escape_compact;
