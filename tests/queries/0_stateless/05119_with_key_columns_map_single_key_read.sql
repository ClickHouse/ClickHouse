-- Tags: no-object-storage, no-random-settings, no-random-merge-tree-settings, no-parallel
-- FileOpen / ReadBytes are local-disk specific and sensitive to mark cache and extra indexes.

-- Accessing one Map key on with_key_columns must open only that key's streams, not the whole Map.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET use_uncompressed_cache = 0;
SET local_filesystem_read_method = 'pread';
SET max_threads = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_wkc_io;
DROP TABLE IF EXISTS t_basic_io;
DROP TABLE IF EXISTS t_buckets_io;

CREATE TABLE t_wkc_io (id UInt64, m Map(String, String))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

CREATE TABLE t_basic_io (id UInt64, m Map(String, String))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

CREATE TABLE t_buckets_io (id UInt64, m Map(String, String))
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

INSERT INTO t_wkc_io
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

INSERT INTO t_basic_io SELECT * FROM t_wkc_io;
INSERT INTO t_buckets_io SELECT * FROM t_wkc_io;

SELECT 'correctness_vs_basic',
    (
        SELECT count()
        FROM
        (
            SELECT id, m['hot'] FROM t_wkc_io
            EXCEPT ALL
            SELECT id, m['hot'] FROM t_basic_io
        )
    );
SELECT 'correctness_vs_buckets',
    (
        SELECT count()
        FROM
        (
            SELECT id, m['hot'] FROM t_wkc_io
            EXCEPT ALL
            SELECT id, m['hot'] FROM t_buckets_io
        )
    );

SELECT 'rewrite_to_key_hot';
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT m['hot'] FROM t_wkc_io)
WHERE explain LIKE '%m.key_hot%';

SYSTEM CLEAR MARK CACHE;
SELECT sum(length(m['hot'])) FROM t_wkc_io FORMAT Null SETTINGS log_comment = '05119_wkc_hot';

SYSTEM CLEAR MARK CACHE;
SELECT sum(length(m['c1'])) FROM t_wkc_io FORMAT Null SETTINGS log_comment = '05119_wkc_c1';

SYSTEM CLEAR MARK CACHE;
SELECT m FROM t_wkc_io FORMAT Null SETTINGS log_comment = '05119_wkc_full';

SYSTEM CLEAR MARK CACHE;
SELECT sum(length(m['hot'])) FROM t_basic_io FORMAT Null SETTINGS log_comment = '05119_basic_hot';

SYSTEM CLEAR MARK CACHE;
SELECT sum(length(m['hot'])) FROM t_buckets_io FORMAT Null SETTINGS log_comment = '05119_buckets_hot';

SYSTEM FLUSH LOGS query_log;

SELECT 'wkc_hot_fewer_files_than_full',
    (
        SELECT ProfileEvents['FileOpen']
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND log_comment = '05119_wkc_hot'
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
            AND log_comment = '05119_wkc_full'
            AND event_date >= yesterday()
            AND event_time >= now() - 600
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

SELECT 'wkc_hot_less_bytes_than_cold_key',
    4 *
    (
        SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes']
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND log_comment = '05119_wkc_hot'
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
            AND log_comment = '05119_wkc_c1'
            AND event_date >= yesterday()
            AND event_time >= now() - 600
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

SELECT 'wkc_hot_less_bytes_than_basic',
    4 *
    (
        SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes']
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND log_comment = '05119_wkc_hot'
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
            AND log_comment = '05119_basic_hot'
            AND event_date >= yesterday()
            AND event_time >= now() - 600
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

SELECT 'wkc_hot_less_bytes_than_buckets',
    4 *
    (
        SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes']
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND log_comment = '05119_wkc_hot'
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
            AND log_comment = '05119_buckets_hot'
            AND event_date >= yesterday()
            AND event_time >= now() - 600
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

SELECT 'wkc_hot_reads_key_subcolumn',
    arrayExists(x -> x LIKE '%key_hot%', columns)
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '05119_wkc_hot'
    AND event_date >= yesterday()
    AND event_time >= now() - 600
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_wkc_io;
DROP TABLE t_basic_io;
DROP TABLE t_buckets_io;
