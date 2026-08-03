-- A part must be written with the serialization versions in effect when it is written, also for the
-- elements of a `Tuple`. The versions of the elements do not appear in the serialization info of the
-- `Tuple` itself (a `Tuple` cannot be sparse, so it carries default settings), so a part must not
-- inherit them from another part that is still in memory: it would write `basic` maps while its
-- `serialization.json` declares `with_buckets` and stop reading back.

drop table if exists t_tuple_map_version;

create table t_tuple_map_version (key UInt64, t Tuple(m Map(String, UInt64)))
engine = MergeTree order by key
settings min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    -- The versions live in `serialization.json`, and both parts need an entry there for `t` at all
    -- (with no entry the versions of the whole file are used directly).
    serialization_info_version = 'with_types',
    ratio_of_defaults_for_sparse_serialization = 0.9375,
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic',
    -- Bucketing unconditional, so the layout depends only on the version.
    max_buckets_in_map = 4, map_buckets_strategy = 'constant', map_buckets_min_avg_size = 0,
    -- Both parts must stay separate and alive.
    max_bytes_to_merge_at_max_space_in_pool = 1;

insert into t_tuple_map_version select number, tuple(map('k' || toString(number % 4), number)) from numbers(1000);

alter table t_tuple_map_version modify setting
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';

insert into t_tuple_map_version select number, tuple(map('k' || toString(number % 4), number)) from numbers(1000, 1000);

-- The bucketed streams of the part written after the ALTER: 1 when it was written with
-- `with_buckets`, 0 when it reused the `basic` serialization of the first part.
select arrayExists(s -> s like '%buckets_info', substreams)
from system.parts_columns
where database = currentDatabase() and table = 't_tuple_map_version' and column = 't' and name = 'all_2_2_0';

select count(), sum(t.m['k1']) from t_tuple_map_version;

drop table t_tuple_map_version;
