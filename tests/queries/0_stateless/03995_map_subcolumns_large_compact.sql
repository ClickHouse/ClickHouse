-- Tags: long
-- Random settings limits: index_granularity=(100, None); index_granularity_bytes=(100000, None)

-- Test: Map subcolumns deserialization from multiple granules, Compact MergeTree parts

-- ==========================================
-- basic serialization, Compact part
-- ==========================================

drop table if exists t;
create table t (id UInt64, m Map(String, UInt64))
engine = MergeTree order by id
settings
    map_serialization_version = 'basic',
    min_bytes_for_wide_part = '200G',
    min_rows_for_wide_part = 1000000,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

insert into t select number, multiIf(
    number < 9000, map('a', number, 'b', number + 1, 'c', number + 2),
    number < 18000, map('a', number, 'b', number + 1),
    number < 27000, map('c', number, 'd', number + 1),
    number < 36000, map('a', number),
    number < 42000, map('a', number, 'b', number + 1, 'c', number + 2, 'd', number + 3, 'e', number + 4),
    map('b', number)
) from numbers(45000);

select 'basic compact: m';
select m from t format Null;
select 'basic compact: m.keys';
select m.keys from t format Null;
select 'basic compact: m.values';
select m.values from t format Null;
select 'basic compact: m.size0';
select m.size0 from t format Null;
select 'basic compact: m.key_a';
select m.key_a from t format Null;
select 'basic compact: m.key_d';
select m.key_d from t format Null;
select 'basic compact: m.key_nonexistent';
select m.key_nonexistent from t format Null;

select 'basic compact: m.keys, m.values';
select m.keys, m.values from t format Null;
select 'basic compact: m.size0, m.key_a';
select m.size0, m.key_a from t format Null;
select 'basic compact: m, m.keys';
select m, m.keys from t format Null;
select 'basic compact: m, m.key_a';
select m, m.key_a from t format Null;
select 'basic compact: m.keys, m';
select m.keys, m from t format Null;
select 'basic compact: m.key_a, m.size0, m';
select m.key_a, m.size0, m from t format Null;
select 'basic compact: m.key_a, m.key_b, m.key_c';
select m.key_a, m.key_b, m.key_c from t format Null;
select 'basic compact: m.key_a, m.key_d';
select m.key_a, m.key_d from t format Null;
select 'basic compact: m, m.keys, m.values, m.size0, m.key_a';
select m, m.keys, m.values, m.size0, m.key_a from t format Null;

drop table t;

-- ==========================================
-- with_buckets serialization, Compact part
-- ==========================================

create table t (id UInt64, m Map(String, UInt64))
engine = MergeTree order by id
settings
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_coefficient = 1.0,
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = '200G',
    min_rows_for_wide_part = 1000000,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

insert into t select number, multiIf(
    number < 9000, map('a', number, 'b', number + 1, 'c', number + 2),
    number < 18000, map('a', number, 'b', number + 1),
    number < 27000, map('c', number, 'd', number + 1),
    number < 36000, map('a', number),
    number < 42000, map('a', number, 'b', number + 1, 'c', number + 2, 'd', number + 3, 'e', number + 4),
    map('b', number)
) from numbers(45000);

select 'with_buckets compact: m';
select m from t format Null;
select 'with_buckets compact: m.keys';
select m.keys from t format Null;
select 'with_buckets compact: m.values';
select m.values from t format Null;
select 'with_buckets compact: m.size0';
select m.size0 from t format Null;
select 'with_buckets compact: m.key_a';
select m.key_a from t format Null;
select 'with_buckets compact: m.key_d';
select m.key_d from t format Null;
select 'with_buckets compact: m.key_nonexistent';
select m.key_nonexistent from t format Null;

select 'with_buckets compact: m.keys, m.values';
select m.keys, m.values from t format Null;
select 'with_buckets compact: m.size0, m.key_a';
select m.size0, m.key_a from t format Null;
select 'with_buckets compact: m, m.keys';
select m, m.keys from t format Null;
select 'with_buckets compact: m, m.key_a';
select m, m.key_a from t format Null;
select 'with_buckets compact: m.keys, m';
select m.keys, m from t format Null;
select 'with_buckets compact: m.key_a, m.size0, m';
select m.key_a, m.size0, m from t format Null;
select 'with_buckets compact: m.key_a, m.key_b, m.key_c';
select m.key_a, m.key_b, m.key_c from t format Null;
select 'with_buckets compact: m.key_a, m.key_d';
select m.key_a, m.key_d from t format Null;
select 'with_buckets compact: m, m.keys, m.values, m.size0, m.key_a';
select m, m.keys, m.values, m.size0, m.key_a from t format Null;

drop table t;

-- ==========================================
-- basic serialization, Compact part, Map inside Tuple
-- ==========================================

create table t (id UInt64, data Tuple(m Map(String, UInt64)))
engine = MergeTree order by id
settings
    map_serialization_version = 'basic',
    min_bytes_for_wide_part = '200G',
    min_rows_for_wide_part = 1000000,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

insert into t select number, tuple(multiIf(
    number < 9000, map('a', number, 'b', number + 1, 'c', number + 2),
    number < 18000, map('a', number, 'b', number + 1),
    number < 27000, map('c', number, 'd', number + 1),
    number < 36000, map('a', number),
    number < 42000, map('a', number, 'b', number + 1, 'c', number + 2, 'd', number + 3, 'e', number + 4),
    map('b', number)
)) from numbers(45000);

select 'basic compact tuple: data.m';
select data.m from t format Null;
select 'basic compact tuple: data.m.keys';
select data.m.keys from t format Null;
select 'basic compact tuple: data.m.values';
select data.m.values from t format Null;
select 'basic compact tuple: data.m.size0';
select data.m.size0 from t format Null;
select 'basic compact tuple: data.m.key_a';
select data.m.key_a from t format Null;

select 'basic compact tuple: data.m.key_a, data.m.key_d';
select data.m.key_a, data.m.key_d from t format Null;
select 'basic compact tuple: data.m, data.m.keys';
select data.m, data.m.keys from t format Null;
select 'basic compact tuple: data.m.keys, data.m';
select data.m.keys, data.m from t format Null;
select 'basic compact tuple: data.m, data.m.keys, data.m.values, data.m.size0, data.m.key_a';
select data.m, data.m.keys, data.m.values, data.m.size0, data.m.key_a from t format Null;

drop table t;

-- ==========================================
-- with_buckets serialization, Compact part, Map inside Tuple
-- ==========================================

create table t (id UInt64, data Tuple(m Map(String, UInt64)))
engine = MergeTree order by id
settings
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_coefficient = 1.0,
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = '200G',
    min_rows_for_wide_part = 1000000,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

insert into t select number, tuple(multiIf(
    number < 9000, map('a', number, 'b', number + 1, 'c', number + 2),
    number < 18000, map('a', number, 'b', number + 1),
    number < 27000, map('c', number, 'd', number + 1),
    number < 36000, map('a', number),
    number < 42000, map('a', number, 'b', number + 1, 'c', number + 2, 'd', number + 3, 'e', number + 4),
    map('b', number)
)) from numbers(45000);

select 'with_buckets compact tuple: data.m';
select data.m from t format Null;
select 'with_buckets compact tuple: data.m.keys';
select data.m.keys from t format Null;
select 'with_buckets compact tuple: data.m.values';
select data.m.values from t format Null;
select 'with_buckets compact tuple: data.m.size0';
select data.m.size0 from t format Null;
select 'with_buckets compact tuple: data.m.key_a';
select data.m.key_a from t format Null;

select 'with_buckets compact tuple: data.m.key_a, data.m.key_d';
select data.m.key_a, data.m.key_d from t format Null;
select 'with_buckets compact tuple: data.m, data.m.keys';
select data.m, data.m.keys from t format Null;
select 'with_buckets compact tuple: data.m.keys, data.m';
select data.m.keys, data.m from t format Null;
select 'with_buckets compact tuple: data.m, data.m.keys, data.m.values, data.m.size0, data.m.key_a';
select data.m, data.m.keys, data.m.values, data.m.size0, data.m.key_a from t format Null;

drop table t;
