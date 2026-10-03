-- Tags: long, no-sanitizers, no-parallel, no-flaky-check, no-parallel-replicas, no-async-insert

-- no-parallel-replicas -- https://github.com/ClickHouse/ClickHouse/issues/90063
-- no-parallel -- the test requires fixed database name and table names to check deduplication blocks in part_log, which makes it incompatible with parallel execution of tests

-- Tags: deduplication blocks have different values for sync and async inserts,
-- async insert calculates it as a hash of data in the block,
-- sync insert uses MergeTreePartWriter's hash which covers only data in the partition.

DROP DATABASE IF EXISTS 03711_database;
CREATE DATABASE 03711_database;

DROP TABLE IF EXISTS 03711_database.03711_join_with;
CREATE TABLE 03711_database.03711_join_with
(
    id UInt32,
    value String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';

SYSTEM STOP MERGES 03711_database.03711_join_with;

INSERT INTO 03711_database.03711_join_with VALUES (1, 'a1'), (1, 'b1'), (1, 'c1');
INSERT INTO 03711_database.03711_join_with VALUES (2, 'a2'), (2, 'b2'), (2, 'c2');

DROP TABLE IF EXISTS 03711_database.03711_table;
CREATE TABLE 03711_database.03711_table
(
    id UInt32
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';

SYSTEM STOP MERGES 03711_database.03711_table;

DROP TABLE IF EXISTS 03711_database.03711_mv_table_1;
CREATE TABLE 03711_database.03711_mv_table_1
(
    id UInt32,
    value String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';

SYSTEM STOP MERGES 03711_database.03711_mv_table_1;

DROP TABLE IF EXISTS 03711_database.03711_mv_table_2;
CREATE TABLE 03711_database.03711_mv_table_2
(
    id UInt32,
    value String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';

SYSTEM STOP MERGES 03711_database.03711_mv_table_2;

DROP TABLE IF EXISTS 03711_database.03711_mv_1;
CREATE MATERIALIZED VIEW 03711_database.03711_mv_1
TO 03711_database.03711_mv_table_1 AS
SELECT r.id as id, r.value as value FROM 03711_database.03711_table as l JOIN 03711_database.03711_join_with as r ON l.id == r.id and l.id = 1;

DROP TABLE IF EXISTS 03711_database.03711_mv_2;
CREATE MATERIALIZED VIEW 03711_database.03711_mv_2
TO 03711_database.03711_mv_table_2 AS
SELECT r.id as id, r.value as value FROM 03711_database.03711_table as l JOIN 03711_database.03711_join_with as r ON l.id == r.id and l.id = 2;

-- Tables with different column types to verify deduplication hash stability
-- across all updateHashWithValueRange overrides.

CREATE TABLE 03711_database.03711_type_decimal (v Decimal64(2)) ENGINE = MergeTree() ORDER BY v
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';
SYSTEM STOP MERGES 03711_database.03711_type_decimal;

CREATE TABLE 03711_database.03711_type_float (v Float64) ENGINE = MergeTree() ORDER BY v
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';
SYSTEM STOP MERGES 03711_database.03711_type_float;

CREATE TABLE 03711_database.03711_type_fixedstr (v FixedString(8)) ENGINE = MergeTree() ORDER BY v
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';
SYSTEM STOP MERGES 03711_database.03711_type_fixedstr;

CREATE TABLE 03711_database.03711_type_string (v String) ENGINE = MergeTree() ORDER BY v
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';
SYSTEM STOP MERGES 03711_database.03711_type_string;

CREATE TABLE 03711_database.03711_type_array (v Array(UInt32)) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';
SYSTEM STOP MERGES 03711_database.03711_type_array;

CREATE TABLE 03711_database.03711_type_nullable (v Nullable(UInt64)) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';
SYSTEM STOP MERGES 03711_database.03711_type_nullable;

CREATE TABLE 03711_database.03711_type_tuple (v Tuple(UInt32, UInt64)) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';
SYSTEM STOP MERGES 03711_database.03711_type_tuple;

CREATE TABLE 03711_database.03711_type_map (v Map(String, UInt32)) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';
SYSTEM STOP MERGES 03711_database.03711_type_map;

CREATE TABLE 03711_database.03711_type_variant (v Variant(UInt64, String, Array(UInt32))) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';
SYSTEM STOP MERGES 03711_database.03711_type_variant;

CREATE TABLE 03711_database.03711_type_dynamic (v Dynamic) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream', dynamic_serialization_version = 'v2';
SYSTEM STOP MERGES 03711_database.03711_type_dynamic;

CREATE TABLE 03711_database.03711_type_json (v JSON) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream', dynamic_serialization_version = 'v2', object_serialization_version = 'v3', object_shared_data_serialization_version = 'map', object_shared_data_serialization_version_for_zero_level_parts = 'map';
SYSTEM STOP MERGES 03711_database.03711_type_json;

CREATE TABLE 03711_database.03711_type_json_mdp0 (v JSON(max_dynamic_paths=0)) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream', dynamic_serialization_version = 'v2', object_serialization_version = 'v3', object_shared_data_serialization_version = 'map', object_shared_data_serialization_version_for_zero_level_parts = 'map';
SYSTEM STOP MERGES 03711_database.03711_type_json_mdp0;

CREATE TABLE 03711_database.03711_type_mixed
(
    id UInt32,
    name String,
    amount Decimal64(2),
    ratio Float64,
    tag FixedString(8),
    tags Array(UInt32),
    nullable_val Nullable(UInt64),
    pair Tuple(UInt32, UInt64),
    kv Map(String, UInt32)
) ENGINE = MergeTree() ORDER BY id
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';
SYSTEM STOP MERGES 03711_database.03711_type_mixed;

-- Async insert tables: async path computes data hash directly from the block
-- (not via MergeTreePartWriter), exercising calculateDataHashBatch.
CREATE TABLE 03711_database.03711_async_uint (v UInt32) ENGINE = MergeTree() ORDER BY v
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';
SYSTEM STOP MERGES 03711_database.03711_async_uint;

CREATE TABLE 03711_database.03711_async_string (v String) ENGINE = MergeTree() ORDER BY v
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';
SYSTEM STOP MERGES 03711_database.03711_async_string;

CREATE TABLE 03711_database.03711_async_array (v Array(UInt32)) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';
SYSTEM STOP MERGES 03711_database.03711_async_array;

CREATE TABLE 03711_database.03711_async_mixed
(
    id UInt32,
    name String,
    amount Decimal64(2),
    ratio Float64,
    tag FixedString(8),
    tags Array(UInt32),
    nullable_val Nullable(UInt64),
    pair Tuple(UInt32, UInt64),
    kv Map(String, UInt32)
) ENGINE = MergeTree() ORDER BY id
    SETTINGS non_replicated_deduplication_window = 1000, min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, index_granularity = 8192, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';
SYSTEM STOP MERGES 03711_database.03711_async_mixed;

SET deduplicate_blocks_in_dependent_materialized_views=1;

SET max_block_size=1;
SET max_insert_block_size=1;
SET min_insert_block_size_rows=0;
SET min_insert_block_size_bytes=0;

INSERT INTO 03711_database.03711_table VALUES (1), (2);

INSERT INTO 03711_database.03711_type_decimal VALUES (1.23), (4.56);
INSERT INTO 03711_database.03711_type_float VALUES (3.14), (2.72);
INSERT INTO 03711_database.03711_type_fixedstr VALUES ('abcdefgh'), ('12345678');
INSERT INTO 03711_database.03711_type_string VALUES ('hello'), ('world');
INSERT INTO 03711_database.03711_type_array VALUES ([1, 2, 3]), ([4, 5]);
INSERT INTO 03711_database.03711_type_nullable VALUES (42), (NULL);
INSERT INTO 03711_database.03711_type_tuple VALUES ((1, 100)), ((2, 200));
INSERT INTO 03711_database.03711_type_map VALUES ({'a': 1, 'b': 2}), ({'c': 3});
INSERT INTO 03711_database.03711_type_variant VALUES (42::UInt64), ('hello'::String);
INSERT INTO 03711_database.03711_type_dynamic VALUES (42::UInt64), ('hello'::String);
INSERT INTO 03711_database.03711_type_json VALUES ('{"a": 1, "b": "hello"}'), ('{"c": [1, 2, 3], "d": 42}');
INSERT INTO 03711_database.03711_type_json_mdp0 VALUES ('{"a": 1, "b": "hello"}'), ('{"c": [1, 2, 3], "d": 42}');
INSERT INTO 03711_database.03711_type_mixed VALUES (1, 'hello', 1.23, 3.14, 'abcdefgh', [1, 2, 3], 42, (1, 100), {'a': 1}), (2, 'world', 4.56, 2.72, '12345678', [4, 5], NULL, (2, 200), {'c': 3});

INSERT INTO 03711_database.03711_async_uint SETTINGS async_insert=1, wait_for_async_insert=1 VALUES (42), (99);
INSERT INTO 03711_database.03711_async_string SETTINGS async_insert=1, wait_for_async_insert=1 VALUES ('hello'), ('world');
INSERT INTO 03711_database.03711_async_array SETTINGS async_insert=1, wait_for_async_insert=1 VALUES ([1, 2, 3]), ([4, 5]);
INSERT INTO 03711_database.03711_async_mixed SETTINGS async_insert=1, wait_for_async_insert=1 VALUES (1, 'hello', 1.23, 3.14, 'abcdefgh', [1, 2, 3], 42, (1, 100), {'a': 1}), (2, 'world', 4.56, 2.72, '12345678', [4, 5], NULL, (2, 200), {'c': 3});

SYSTEM FLUSH LOGS part_log;

SELECT table, name, argMax(part_type, event_time_microseconds), argMax(deduplication_block_ids, event_time_microseconds) FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND
    table IN ['03711_join_with', '03711_table', '03711_mv_table_1', '03711_mv_table_2',
               '03711_type_decimal', '03711_type_float', '03711_type_fixedstr', '03711_type_string',
               '03711_type_array', '03711_type_nullable', '03711_type_tuple', '03711_type_map',
               '03711_type_variant', '03711_type_dynamic', '03711_type_json', '03711_type_json_mdp0',
               '03711_type_mixed',
               '03711_async_uint', '03711_async_string', '03711_async_array', '03711_async_mixed']
    AND database = '03711_database'
group BY database, table, name
ORDER BY ALL;

DROP DATABASE 03711_database;
