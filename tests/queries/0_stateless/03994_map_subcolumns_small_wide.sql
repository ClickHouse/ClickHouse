-- Test: Map subcolumns reading with small dataset and Wide MergeTree parts

drop table if exists source;
create table source (id UInt64, m Map(String, UInt64)) engine = Memory;
insert into source values
    (1, {'a' : 1, 'b' : 2, 'c' : 3}),
    (2, {'a' : 10, 'b' : 20}),
    (3, {}),
    (4, {'c' : 100, 'd' : 200}),
    (5, {'a' : 5}),
    (6, {'a' : 1, 'b' : 2, 'c' : 3, 'd' : 4, 'e' : 5}),
    (7, {'b' : 42}),
    (8, {'a' : 0, 'd' : 0}),
    (9, {}),
    (10, {'a' : 99, 'b' : 98, 'c' : 97, 'd' : 96, 'e' : 95});

-- ==========================================
-- basic serialization, Wide part
-- ==========================================

drop table if exists t;
create table t (id UInt64, m Map(String, UInt64))
engine = MergeTree order by id
settings
    map_serialization_version = 'basic',
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

insert into t select * from source;

select 'basic wide: m';
select m from t order by id;
select 'basic wide: m.keys';
select m.keys from t order by id;
select 'basic wide: m.values';
select m.values from t order by id;
select 'basic wide: m.size0';
select m.size0 from t order by id;
select 'basic wide: m.key_a';
select m.key_a from t order by id;
select 'basic wide: m.key_d';
select m.key_d from t order by id;
select 'basic wide: m.key_nonexistent';
select m.key_nonexistent from t order by id;

select 'basic wide: m.keys, m.values';
select m.keys, m.values from t order by id;
select 'basic wide: m.size0, m.key_a';
select m.size0, m.key_a from t order by id;
select 'basic wide: m, m.keys';
select m, m.keys from t order by id;
select 'basic wide: m, m.key_a';
select m, m.key_a from t order by id;
select 'basic wide: m.keys, m';
select m.keys, m from t order by id;
select 'basic wide: m.key_a, m.size0, m';
select m.key_a, m.size0, m from t order by id;
select 'basic wide: m.key_a, m.key_b, m.key_c';
select m.key_a, m.key_b, m.key_c from t order by id;
select 'basic wide: m.key_a, m.key_d';
select m.key_a, m.key_d from t order by id;
select 'basic wide: m, m.keys, m.values, m.size0, m.key_a';
select m, m.keys, m.values, m.size0, m.key_a from t order by id;

select 'basic wide: m, m.keys, m.values, m.size0, m.key_a limit 3';
select m, m.keys, m.values, m.size0, m.key_a from t order by id limit 3;
select 'basic wide: m, m.keys, m.values, m.size0, m.key_a max_block_size=3';
select m, m.keys, m.values, m.size0, m.key_a from t order by id settings max_block_size=3;

drop table t;

-- ==========================================
-- with_buckets serialization, Wide part
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
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

insert into t select * from source;

select 'with_buckets wide: m';
select m from t order by id;
select 'with_buckets wide: m.keys';
select m.keys from t order by id;
select 'with_buckets wide: m.values';
select m.values from t order by id;
select 'with_buckets wide: m.size0';
select m.size0 from t order by id;
select 'with_buckets wide: m.key_a';
select m.key_a from t order by id;
select 'with_buckets wide: m.key_d';
select m.key_d from t order by id;
select 'with_buckets wide: m.key_nonexistent';
select m.key_nonexistent from t order by id;

select 'with_buckets wide: m.keys, m.values';
select m.keys, m.values from t order by id;
select 'with_buckets wide: m.size0, m.key_a';
select m.size0, m.key_a from t order by id;
select 'with_buckets wide: m, m.keys';
select m, m.keys from t order by id;
select 'with_buckets wide: m, m.key_a';
select m, m.key_a from t order by id;
select 'with_buckets wide: m.keys, m';
select m.keys, m from t order by id;
select 'with_buckets wide: m.key_a, m.size0, m';
select m.key_a, m.size0, m from t order by id;
select 'with_buckets wide: m.key_a, m.key_b, m.key_c';
select m.key_a, m.key_b, m.key_c from t order by id;
select 'with_buckets wide: m.key_a, m.key_d';
select m.key_a, m.key_d from t order by id;
select 'with_buckets wide: m, m.keys, m.values, m.size0, m.key_a';
select m, m.keys, m.values, m.size0, m.key_a from t order by id;

select 'with_buckets wide: m, m.keys, m.values, m.size0, m.key_a limit 3';
select m, m.keys, m.values, m.size0, m.key_a from t order by id limit 3;
select 'with_buckets wide: m, m.keys, m.values, m.size0, m.key_a max_block_size=3';
select m, m.keys, m.values, m.size0, m.key_a from t order by id settings max_block_size=3;

drop table t;

-- ==========================================
-- basic serialization, Wide part, Map inside Tuple
-- ==========================================

create table t (id UInt64, data Tuple(m Map(String, UInt64)))
engine = MergeTree order by id
settings
    map_serialization_version = 'basic',
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

insert into t select id, tuple(m) from source;

select 'basic wide tuple: data.m';
select data.m from t order by id;
select 'basic wide tuple: data.m.keys';
select data.m.keys from t order by id;
select 'basic wide tuple: data.m.values';
select data.m.values from t order by id;
select 'basic wide tuple: data.m.size0';
select data.m.size0 from t order by id;
select 'basic wide tuple: data.m.key_a';
select data.m.key_a from t order by id;

select 'basic wide tuple: data.m.key_a, data.m.key_d';
select data.m.key_a, data.m.key_d from t order by id;
select 'basic wide tuple: data.m, data.m.keys';
select data.m, data.m.keys from t order by id;
select 'basic wide tuple: data.m.keys, data.m';
select data.m.keys, data.m from t order by id;
select 'basic wide tuple: data.m, data.m.keys, data.m.values, data.m.size0, data.m.key_a';
select data.m, data.m.keys, data.m.values, data.m.size0, data.m.key_a from t order by id;

drop table t;

-- ==========================================
-- with_buckets serialization, Wide part, Map inside Tuple
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
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

insert into t select id, tuple(m) from source;

select 'with_buckets wide tuple: data.m';
select data.m from t order by id;
select 'with_buckets wide tuple: data.m.keys';
select data.m.keys from t order by id;
select 'with_buckets wide tuple: data.m.values';
select data.m.values from t order by id;
select 'with_buckets wide tuple: data.m.size0';
select data.m.size0 from t order by id;
select 'with_buckets wide tuple: data.m.key_a';
select data.m.key_a from t order by id;

select 'with_buckets wide tuple: data.m.key_a, data.m.key_d';
select data.m.key_a, data.m.key_d from t order by id;
select 'with_buckets wide tuple: data.m, data.m.keys';
select data.m, data.m.keys from t order by id;
select 'with_buckets wide tuple: data.m.keys, data.m';
select data.m.keys, data.m from t order by id;
select 'with_buckets wide tuple: data.m, data.m.keys, data.m.values, data.m.size0, data.m.key_a';
select data.m, data.m.keys, data.m.values, data.m.size0, data.m.key_a from t order by id;

drop table t;
drop table source;
