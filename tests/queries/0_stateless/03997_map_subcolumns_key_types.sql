-- Test: Map subcolumns with different key types

-- ==========================================
-- basic serialization
-- ==========================================

drop table if exists t;
create table t
(
    id UInt64,
    m_uint Map(UInt64, String),
    m_int Map(Int32, String),
    m_date Map(Date, UInt64),
    m_uuid Map(UUID, UInt64),
    m_ipv4 Map(IPv4, UInt64),
    m_fs Map(FixedString(3), UInt64),
    m_lc Map(LowCardinality(String), UInt64)
)
engine = MergeTree order by id
settings
    map_serialization_version = 'basic',
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

insert into t values
    (1, {1 : 'a', 2 : 'b', 3 : 'c'}, {-1 : 'neg', 0 : 'zero', 1 : 'pos'}, {'2024-01-01' : 10, '2024-06-15' : 20}, {'550e8400-e29b-41d4-a716-446655440000' : 1, '6ba7b810-9dad-11d1-80b4-00c04fd430c8' : 2}, {'192.168.1.1' : 100, '10.0.0.1' : 200}, {'abc' : 1, 'def' : 2}, {'foo' : 1, 'bar' : 2}),
    (2, {2 : 'x', 4 : 'y'}, {-100 : 'x', 100 : 'y'}, {'2024-01-01' : 30}, {'550e8400-e29b-41d4-a716-446655440000' : 10}, {'192.168.1.1' : 300}, {'abc' : 10, 'xyz' : 20}, {'foo' : 10, 'baz' : 20}),
    (3, {}, {}, {}, {}, {}, {}, {});

select 'basic UInt64 key: m_uint.key_1';
select m_uint.key_1 from t order by id;
select 'basic UInt64 key: m_uint[1]';
select m_uint[1] from t order by id;
select 'basic UInt64 key: m_uint.key_2';
select m_uint.key_2 from t order by id;
select 'basic UInt64 key: m_uint[2]';
select m_uint[2] from t order by id;
select 'basic UInt64 key: m_uint.key_4';
select m_uint.key_4 from t order by id;
select 'basic UInt64 key: m_uint[4]';
select m_uint[4] from t order by id;
select 'basic UInt64 key: m_uint.key_999';
select m_uint.key_999 from t order by id;
select 'basic UInt64 key: m_uint[999]';
select m_uint[999] from t order by id;
select 'basic UInt64 key: m_uint.key_1, m_uint.key_4';
select m_uint.key_1, m_uint.key_4 from t order by id;
select 'basic UInt64 key: m_uint[1], m_uint[4]';
select m_uint[1], m_uint[4] from t order by id;

select 'basic Int32 key: m_int.key_-1';
select `m_int.key_-1` from t order by id;
select 'basic Int32 key: m_int[-1]';
select m_int[-1] from t order by id;
select 'basic Int32 key: m_int.key_0';
select m_int.key_0 from t order by id;
select 'basic Int32 key: m_int[0]';
select m_int[0] from t order by id;
select 'basic Int32 key: m_int.key_1';
select m_int.key_1 from t order by id;
select 'basic Int32 key: m_int[1]';
select m_int[1] from t order by id;
select 'basic Int32 key: m_int.key_-100';
select `m_int.key_-100` from t order by id;
select 'basic Int32 key: m_int[-100]';
select m_int[-100] from t order by id;

select 'basic Date key: m_date.key_2024-01-01';
select `m_date.key_2024-01-01` from t order by id;
select 'basic Date key: m_date[toDate(''2024-01-01'')]';
select m_date[toDate('2024-01-01')] from t order by id;
select 'basic Date key: m_date.key_2024-06-15';
select `m_date.key_2024-06-15` from t order by id;
select 'basic Date key: m_date[toDate(''2024-06-15'')]';
select m_date[toDate('2024-06-15')] from t order by id;
select 'basic Date key: m_date.key_2099-12-31';
select `m_date.key_2099-12-31` from t order by id;
select 'basic Date key: m_date[toDate(''2099-12-31'')]';
select m_date[toDate('2099-12-31')] from t order by id;

select 'basic UUID key: m_uuid.key_550e8400-e29b-41d4-a716-446655440000';
select `m_uuid.key_550e8400-e29b-41d4-a716-446655440000` from t order by id;
select 'basic UUID key: m_uuid[toUUID(''550e8400-e29b-41d4-a716-446655440000'')]';
select m_uuid[toUUID('550e8400-e29b-41d4-a716-446655440000')] from t order by id;
select 'basic UUID key: m_uuid.key_6ba7b810-9dad-11d1-80b4-00c04fd430c8';
select `m_uuid.key_6ba7b810-9dad-11d1-80b4-00c04fd430c8` from t order by id;
select 'basic UUID key: m_uuid[toUUID(''6ba7b810-9dad-11d1-80b4-00c04fd430c8'')]';
select m_uuid[toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')] from t order by id;

select 'basic IPv4 key: m_ipv4.key_192.168.1.1';
select `m_ipv4.key_192.168.1.1` from t order by id;
select 'basic IPv4 key: m_ipv4[toIPv4(''192.168.1.1'')]';
select m_ipv4[toIPv4('192.168.1.1')] from t order by id;
select 'basic IPv4 key: m_ipv4.key_10.0.0.1';
select `m_ipv4.key_10.0.0.1` from t order by id;
select 'basic IPv4 key: m_ipv4[toIPv4(''10.0.0.1'')]';
select m_ipv4[toIPv4('10.0.0.1')] from t order by id;
select 'basic IPv4 key: m_ipv4.key_127.0.0.1';
select `m_ipv4.key_127.0.0.1` from t order by id;
select 'basic IPv4 key: m_ipv4[toIPv4(''127.0.0.1'')]';
select m_ipv4[toIPv4('127.0.0.1')] from t order by id;

select 'basic FixedString key: m_fs.key_abc';
select m_fs.key_abc from t order by id;
select 'basic FixedString key: m_fs[''abc'']';
select m_fs['abc'] from t order by id;
select 'basic FixedString key: m_fs.key_def';
select m_fs.key_def from t order by id;
select 'basic FixedString key: m_fs[''def'']';
select m_fs['def'] from t order by id;
select 'basic FixedString key: m_fs.key_zzz';
select m_fs.key_zzz from t order by id;
select 'basic FixedString key: m_fs[''zzz'']';
select m_fs['zzz'] from t order by id;

select 'basic LowCardinality(String) key: m_lc.key_foo';
select m_lc.key_foo from t order by id;
select 'basic LowCardinality(String) key: m_lc[''foo'']';
select m_lc['foo'] from t order by id;
select 'basic LowCardinality(String) key: m_lc.key_bar';
select m_lc.key_bar from t order by id;
select 'basic LowCardinality(String) key: m_lc[''bar'']';
select m_lc['bar'] from t order by id;
select 'basic LowCardinality(String) key: m_lc.key_missing';
select m_lc.key_missing from t order by id;
select 'basic LowCardinality(String) key: m_lc[''missing'']';
select m_lc['missing'] from t order by id;

drop table t;

-- ==========================================
-- with_buckets serialization
-- ==========================================

create table t
(
    id UInt64,
    m_uint Map(UInt64, String),
    m_int Map(Int32, String),
    m_date Map(Date, UInt64),
    m_uuid Map(UUID, UInt64),
    m_ipv4 Map(IPv4, UInt64),
    m_fs Map(FixedString(3), UInt64),
    m_lc Map(LowCardinality(String), UInt64)
)
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

insert into t values
    (1, {1 : 'a', 2 : 'b', 3 : 'c'}, {-1 : 'neg', 0 : 'zero', 1 : 'pos'}, {'2024-01-01' : 10, '2024-06-15' : 20}, {'550e8400-e29b-41d4-a716-446655440000' : 1, '6ba7b810-9dad-11d1-80b4-00c04fd430c8' : 2}, {'192.168.1.1' : 100, '10.0.0.1' : 200}, {'abc' : 1, 'def' : 2}, {'foo' : 1, 'bar' : 2}),
    (2, {2 : 'x', 4 : 'y'}, {-100 : 'x', 100 : 'y'}, {'2024-01-01' : 30}, {'550e8400-e29b-41d4-a716-446655440000' : 10}, {'192.168.1.1' : 300}, {'abc' : 10, 'xyz' : 20}, {'foo' : 10, 'baz' : 20}),
    (3, {}, {}, {}, {}, {}, {}, {});

select 'with_buckets UInt64 key: m_uint.key_1';
select m_uint.key_1 from t order by id;
select 'with_buckets UInt64 key: m_uint[1]';
select m_uint[1] from t order by id;
select 'with_buckets UInt64 key: m_uint.key_4';
select m_uint.key_4 from t order by id;
select 'with_buckets UInt64 key: m_uint[4]';
select m_uint[4] from t order by id;
select 'with_buckets UInt64 key: m_uint.key_1, m_uint.key_4';
select m_uint.key_1, m_uint.key_4 from t order by id;
select 'with_buckets UInt64 key: m_uint[1], m_uint[4]';
select m_uint[1], m_uint[4] from t order by id;

select 'with_buckets Int32 key: m_int.key_-1';
select `m_int.key_-1` from t order by id;
select 'with_buckets Int32 key: m_int[-1]';
select m_int[-1] from t order by id;
select 'with_buckets Int32 key: m_int.key_0';
select m_int.key_0 from t order by id;
select 'with_buckets Int32 key: m_int[0]';
select m_int[0] from t order by id;

select 'with_buckets Date key: m_date.key_2024-01-01';
select `m_date.key_2024-01-01` from t order by id;
select 'with_buckets Date key: m_date[toDate(''2024-01-01'')]';
select m_date[toDate('2024-01-01')] from t order by id;
select 'with_buckets Date key: m_date.key_2024-06-15';
select `m_date.key_2024-06-15` from t order by id;
select 'with_buckets Date key: m_date[toDate(''2024-06-15'')]';
select m_date[toDate('2024-06-15')] from t order by id;

select 'with_buckets UUID key: m_uuid.key_550e8400-e29b-41d4-a716-446655440000';
select `m_uuid.key_550e8400-e29b-41d4-a716-446655440000` from t order by id;
select 'with_buckets UUID key: m_uuid[toUUID(''550e8400-e29b-41d4-a716-446655440000'')]';
select m_uuid[toUUID('550e8400-e29b-41d4-a716-446655440000')] from t order by id;
select 'with_buckets UUID key: m_uuid.key_6ba7b810-9dad-11d1-80b4-00c04fd430c8';
select `m_uuid.key_6ba7b810-9dad-11d1-80b4-00c04fd430c8` from t order by id;
select 'with_buckets UUID key: m_uuid[toUUID(''6ba7b810-9dad-11d1-80b4-00c04fd430c8'')]';
select m_uuid[toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')] from t order by id;

select 'with_buckets IPv4 key: m_ipv4.key_192.168.1.1';
select `m_ipv4.key_192.168.1.1` from t order by id;
select 'with_buckets IPv4 key: m_ipv4[toIPv4(''192.168.1.1'')]';
select m_ipv4[toIPv4('192.168.1.1')] from t order by id;
select 'with_buckets IPv4 key: m_ipv4.key_10.0.0.1';
select `m_ipv4.key_10.0.0.1` from t order by id;
select 'with_buckets IPv4 key: m_ipv4[toIPv4(''10.0.0.1'')]';
select m_ipv4[toIPv4('10.0.0.1')] from t order by id;

select 'with_buckets FixedString key: m_fs.key_abc';
select m_fs.key_abc from t order by id;
select 'with_buckets FixedString key: m_fs[''abc'']';
select m_fs['abc'] from t order by id;
select 'with_buckets FixedString key: m_fs.key_def';
select m_fs.key_def from t order by id;
select 'with_buckets FixedString key: m_fs[''def'']';
select m_fs['def'] from t order by id;

select 'with_buckets LowCardinality(String) key: m_lc.key_foo';
select m_lc.key_foo from t order by id;
select 'with_buckets LowCardinality(String) key: m_lc[''foo'']';
select m_lc['foo'] from t order by id;
select 'with_buckets LowCardinality(String) key: m_lc.key_baz';
select m_lc.key_baz from t order by id;
select 'with_buckets LowCardinality(String) key: m_lc[''baz'']';
select m_lc['baz'] from t order by id;

drop table t;
