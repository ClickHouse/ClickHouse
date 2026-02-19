-- Tags: no-parallel

SET alter_sync = 2;
SET optimize_throw_if_noop = 0;

-- Test 1: Basic TTL delete with vertical merge (mix of expired and non-expired rows)
DROP TABLE IF EXISTS t_ttl_vert_1;

CREATE TABLE t_ttl_vert_1
(
    id UInt64,
    d DateTime DEFAULT '2000-01-01 00:00:00',
    c1 UInt64,
    c2 UInt64,
    c3 UInt64,
    c4 UInt64
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 1 DAY
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

SYSTEM STOP TTL MERGES t_ttl_vert_1;
SYSTEM STOP MERGES t_ttl_vert_1;

INSERT INTO t_ttl_vert_1 SELECT number, '2000-01-01 00:00:00', number * 10, number * 100, number * 1000, number * 10000 FROM numbers(1000);
INSERT INTO t_ttl_vert_1 SELECT number + 1000, now() + INTERVAL 1 YEAR, number * 10, number * 100, number * 1000, number * 10000 FROM numbers(1000);

SELECT 'test1_before', count() FROM t_ttl_vert_1;

SYSTEM START TTL MERGES t_ttl_vert_1;
SYSTEM START MERGES t_ttl_vert_1;
OPTIMIZE TABLE t_ttl_vert_1 FINAL;

SELECT 'test1_after', count() FROM t_ttl_vert_1;
SELECT 'test1_min_id', min(id) FROM t_ttl_vert_1;
SELECT 'test1_max_id', max(id) FROM t_ttl_vert_1;
SELECT 'test1_check_cols', sum(c1), sum(c2), sum(c3), sum(c4) FROM t_ttl_vert_1;
SELECT 'test1_parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_vert_1' AND active;

SYSTEM FLUSH LOGS part_log;
SELECT 'test1_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_1' AND event_type = 'MergeParts'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE IF EXISTS t_ttl_vert_1;

-- Test 2: All rows expired
DROP TABLE IF EXISTS t_ttl_vert_2;

CREATE TABLE t_ttl_vert_2
(
    id UInt64,
    d DateTime DEFAULT '2000-01-01 00:00:00',
    c1 UInt64,
    c2 UInt64,
    c3 UInt64,
    c4 UInt64
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 1 DAY
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

SYSTEM STOP TTL MERGES t_ttl_vert_2;
SYSTEM STOP MERGES t_ttl_vert_2;

INSERT INTO t_ttl_vert_2 SELECT number, '2000-01-01 00:00:00', rand(), rand(), rand(), rand() FROM numbers(1000);
INSERT INTO t_ttl_vert_2 SELECT number + 1000, '2000-01-02 00:00:00', rand(), rand(), rand(), rand() FROM numbers(1000);

SELECT 'test2_before', count() FROM t_ttl_vert_2;

SYSTEM START TTL MERGES t_ttl_vert_2;
SYSTEM START MERGES t_ttl_vert_2;
OPTIMIZE TABLE t_ttl_vert_2 FINAL;

SELECT 'test2_after', count() FROM t_ttl_vert_2;

SYSTEM FLUSH LOGS part_log;
SELECT 'test2_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_2' AND event_type = 'MergeParts'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE IF EXISTS t_ttl_vert_2;

-- Test 3: Disabled setting falls back to horizontal merge (still produces correct results)
DROP TABLE IF EXISTS t_ttl_vert_3;

CREATE TABLE t_ttl_vert_3
(
    id UInt64,
    d DateTime DEFAULT '2000-01-01 00:00:00',
    c1 UInt64,
    c2 UInt64,
    c3 UInt64,
    c4 UInt64
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 1 DAY
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 0,
    merge_with_ttl_timeout = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

SYSTEM STOP TTL MERGES t_ttl_vert_3;
SYSTEM STOP MERGES t_ttl_vert_3;

INSERT INTO t_ttl_vert_3 SELECT number, '2000-01-01 00:00:00', number * 10, number * 100, number * 1000, number * 10000 FROM numbers(1000);
INSERT INTO t_ttl_vert_3 SELECT number + 1000, now() + INTERVAL 1 YEAR, number * 10, number * 100, number * 1000, number * 10000 FROM numbers(1000);

SELECT 'test3_before', count() FROM t_ttl_vert_3;

SYSTEM START TTL MERGES t_ttl_vert_3;
SYSTEM START MERGES t_ttl_vert_3;
OPTIMIZE TABLE t_ttl_vert_3 FINAL;

SELECT 'test3_after', count() FROM t_ttl_vert_3;
SELECT 'test3_check_cols', sum(c1), sum(c2), sum(c3), sum(c4) FROM t_ttl_vert_3;

SYSTEM FLUSH LOGS part_log;
SELECT 'test3_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_3' AND event_type = 'MergeParts'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE IF EXISTS t_ttl_vert_3;

-- Test 4: TTL with WHERE clause and vertical merge
DROP TABLE IF EXISTS t_ttl_vert_4;

CREATE TABLE t_ttl_vert_4
(
    id UInt64,
    d DateTime,
    category UInt8,
    c1 UInt64,
    c2 UInt64,
    c3 UInt64
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 1 DAY DELETE WHERE category = 1
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

SYSTEM STOP TTL MERGES t_ttl_vert_4;
SYSTEM STOP MERGES t_ttl_vert_4;

INSERT INTO t_ttl_vert_4 SELECT number, '2000-01-01 00:00:00', number % 2, number * 10, number * 100, number * 1000 FROM numbers(1000);
INSERT INTO t_ttl_vert_4 SELECT number + 1000, '2000-01-01 00:00:00', number % 2, number * 10, number * 100, number * 1000 FROM numbers(1000);

SELECT 'test4_before', count() FROM t_ttl_vert_4;

SYSTEM START TTL MERGES t_ttl_vert_4;
SYSTEM START MERGES t_ttl_vert_4;
OPTIMIZE TABLE t_ttl_vert_4 FINAL;

SELECT 'test4_after', count() FROM t_ttl_vert_4;
SELECT 'test4_category', uniq(category) FROM t_ttl_vert_4;
SELECT 'test4_parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_vert_4' AND active;

SYSTEM FLUSH LOGS part_log;
SELECT 'test4_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_4' AND event_type = 'MergeParts'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE IF EXISTS t_ttl_vert_4;
