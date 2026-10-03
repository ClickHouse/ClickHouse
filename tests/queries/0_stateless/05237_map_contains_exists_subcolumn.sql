-- Tags: no-object-storage, no-random-settings, no-random-merge-tree-settings, no-parallel
-- FileOpen / ReadBytes are local-disk specific and sensitive to mark cache and extra indexes.

-- A constant mapContains on with_key_columns reads that key's .exists_ stream.
-- basic and with_buckets keep has(m.keys). A basic part inside a with_key_columns table
-- still returns the same 0/1 by scanning that part's keys.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET optimize_rewrite_has_to_in = 0;
SET use_uncompressed_cache = 0;
SET local_filesystem_read_method = 'pread';
SET max_threads = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_wkc_contains;
DROP TABLE IF EXISTS t_basic_contains;
DROP TABLE IF EXISTS t_buckets_contains;
DROP TABLE IF EXISTS t_mixed_contains;

CREATE TABLE t_wkc_contains (id UInt64, m Map(String, String))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

CREATE TABLE t_basic_contains (id UInt64, m Map(String, String))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

CREATE TABLE t_buckets_contains (id UInt64, m Map(String, String))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

CREATE TABLE t_mixed_contains (id UInt64, m Map(String, String))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'basic',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

INSERT INTO t_wkc_contains
SELECT
    number,
    map(
        'hot', toString(number),
        'c1', repeat('x', 20000),
        'c2', repeat('y', 20000),
        'c3', repeat('z', 20000),
        'c4', repeat('a', 20000),
        'c5', repeat('b', 20000),
        'c6', repeat('c', 20000),
        'c7', repeat('d', 20000),
        'c8', repeat('e', 20000))
FROM numbers(100);

INSERT INTO t_wkc_contains VALUES (1000, map('c1', 'only'));

INSERT INTO t_basic_contains SELECT * FROM t_wkc_contains;
INSERT INTO t_buckets_contains SELECT * FROM t_wkc_contains;
INSERT INTO t_mixed_contains SELECT * FROM t_wkc_contains;

SELECT 'correctness_vs_basic',
    (
        SELECT count()
        FROM
        (
            SELECT id, mapContains(m, 'hot'), mapContains(m, 'nope'), has(m, 'c1'), notHas(m, 'hot'), mapContainsKey(m, 'c1')
            FROM t_wkc_contains
            EXCEPT ALL
            SELECT id, mapContains(m, 'hot'), mapContains(m, 'nope'), has(m, 'c1'), notHas(m, 'hot'), mapContainsKey(m, 'c1')
            FROM t_basic_contains
        )
    ) = 0;

SELECT 'correctness_vs_buckets',
    (
        SELECT count()
        FROM
        (
            SELECT id, mapContains(m, 'hot'), mapContains(m, 'nope'), has(m, 'c1'), notHas(m, 'hot')
            FROM t_wkc_contains
            EXCEPT ALL
            SELECT id, mapContains(m, 'hot'), mapContains(m, 'nope'), has(m, 'c1'), notHas(m, 'hot')
            FROM t_buckets_contains
        )
    ) = 0;

SELECT 'mixed_basic_part_matches',
    (
        SELECT count()
        FROM
        (
            SELECT id, mapContains(m, 'hot'), mapContains(m, 'nope'), has(m, 'c1'), notHas(m, 'hot')
            FROM t_mixed_contains
            EXCEPT ALL
            SELECT id, mapContains(m, 'hot'), mapContains(m, 'nope'), has(m, 'c1'), notHas(m, 'hot')
            FROM t_basic_contains
        )
    ) = 0;

OPTIMIZE TABLE t_mixed_contains FINAL;

SELECT 'mixed_merged_part_matches',
    (
        SELECT count()
        FROM
        (
            SELECT id, mapContains(m, 'hot'), mapContains(m, 'nope'), has(m, 'c1'), notHas(m, 'hot')
            FROM t_mixed_contains
            EXCEPT ALL
            SELECT id, mapContains(m, 'hot'), mapContains(m, 'nope'), has(m, 'c1'), notHas(m, 'hot')
            FROM t_basic_contains
        )
    ) = 0;

SELECT 'wkc_map_contains_rewritten', count() > 0
FROM (EXPLAIN actions = 1 SELECT mapContains(m, 'hot') FROM t_wkc_contains)
WHERE explain LIKE '%m.exists_hot%';

SELECT 'wkc_has_rewritten', count() > 0
FROM (EXPLAIN actions = 1 SELECT has(m, 'hot') FROM t_wkc_contains)
WHERE explain LIKE '%m.exists_hot%';

SELECT 'wkc_not_has_rewritten', count() > 0
FROM (EXPLAIN actions = 1 SELECT notHas(m, 'hot') FROM t_wkc_contains)
WHERE explain LIKE '%m.exists_hot%';

SELECT 'wkc_nonconst_keeps_keys', count() > 0
FROM (EXPLAIN actions = 1 SELECT mapContains(m, materialize('hot')) FROM t_wkc_contains)
WHERE explain LIKE '%m.keys%';

SELECT 'basic_keeps_keys',
    (
        SELECT count() > 0
        FROM (EXPLAIN actions = 1 SELECT mapContains(m, 'hot') FROM t_basic_contains)
        WHERE explain LIKE '%m.keys%'
    )
    AND
    (
        SELECT count() = 0
        FROM (EXPLAIN actions = 1 SELECT mapContains(m, 'hot') FROM t_basic_contains)
        WHERE explain LIKE '%m.exists_hot%'
    );

SYSTEM CLEAR MARK CACHE;
SELECT countIf(mapContains(m, 'hot')) FROM t_wkc_contains FORMAT Null SETTINGS log_comment = '05237_wkc_contains_hot';

SYSTEM CLEAR MARK CACHE;
SELECT m FROM t_wkc_contains FORMAT Null SETTINGS log_comment = '05237_wkc_full';

SYSTEM CLEAR MARK CACHE;
SELECT sum(length(m['c1'])) FROM t_wkc_contains FORMAT Null SETTINGS log_comment = '05237_wkc_c1';

SYSTEM FLUSH LOGS query_log;

SELECT 'wkc_contains_fewer_files_than_full',
    (
        SELECT ProfileEvents['FileOpen']
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND log_comment = '05237_wkc_contains_hot'
            AND event_date >= yesterday()
            AND event_time >= now() - 600
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    )
    <
    (
        SELECT ProfileEvents['FileOpen']
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND log_comment = '05237_wkc_full'
            AND event_date >= yesterday()
            AND event_time >= now() - 600
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

SELECT 'wkc_contains_less_bytes_than_full',
    4 *
    (
        SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes']
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND log_comment = '05237_wkc_contains_hot'
            AND event_date >= yesterday()
            AND event_time >= now() - 600
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    )
    <
    (
        SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes']
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND log_comment = '05237_wkc_full'
            AND event_date >= yesterday()
            AND event_time >= now() - 600
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

SELECT 'wkc_contains_less_bytes_than_cold_value',
    4 *
    (
        SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes']
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND log_comment = '05237_wkc_contains_hot'
            AND event_date >= yesterday()
            AND event_time >= now() - 600
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    )
    <
    (
        SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes']
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND log_comment = '05237_wkc_c1'
            AND event_date >= yesterday()
            AND event_time >= now() - 600
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

SELECT 'wkc_contains_reads_exists_stream',
    arrayExists(x -> x LIKE '%exists_hot%', columns)
    AND NOT arrayExists(x -> x LIKE '%key_c1%' OR x LIKE '%key_hot%', columns)
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '05237_wkc_contains_hot'
    AND event_date >= yesterday()
    AND event_time >= now() - 600
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_wkc_contains;
DROP TABLE t_basic_contains;
DROP TABLE t_buckets_contains;
DROP TABLE t_mixed_contains;
