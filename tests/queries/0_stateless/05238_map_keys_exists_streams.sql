-- Tags: no-object-storage, no-random-settings, no-random-merge-tree-settings, no-parallel
-- FileOpen / ReadBytes are local-disk specific and sensitive to mark cache and extra indexes.

-- mapKeys on with_key_columns reads .exists_ streams and names keys from the manifest.
-- The key array matches the full-column read, including rows that omit a key.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET use_uncompressed_cache = 0;
SET local_filesystem_read_method = 'pread';
SET max_threads = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_wkc_keys;

CREATE TABLE t_wkc_keys (id UInt64, m Map(String, String))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

INSERT INTO t_wkc_keys
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

INSERT INTO t_wkc_keys VALUES (1000, map('c1', 'only'));

SELECT 'keys_match_full_read',
    (
        SELECT count()
        FROM
        (
            SELECT id, mapKeys(m) FROM t_wkc_keys SETTINGS optimize_functions_to_subcolumns = 0
            EXCEPT ALL
            SELECT id, mapKeys(m) FROM t_wkc_keys SETTINGS optimize_functions_to_subcolumns = 1
        )
    ) = 0;

SELECT 'values_match_full_read',
    (
        SELECT count()
        FROM
        (
            SELECT id, mapValues(m) FROM t_wkc_keys SETTINGS optimize_functions_to_subcolumns = 0
            EXCEPT ALL
            SELECT id, mapValues(m) FROM t_wkc_keys SETTINGS optimize_functions_to_subcolumns = 1
        )
    ) = 0;

SELECT 'absent_row_omits_hot',
    (SELECT mapKeys(m) FROM t_wkc_keys WHERE id = 1000) = ['c1'];

SYSTEM CLEAR MARK CACHE;
SELECT mapKeys(m) FROM t_wkc_keys FORMAT Null SETTINGS log_comment = '05238_map_keys';

SYSTEM CLEAR MARK CACHE;
SELECT m FROM t_wkc_keys FORMAT Null SETTINGS log_comment = '05238_full';

SYSTEM CLEAR MARK CACHE;
SELECT sum(length(m['c1'])) FROM t_wkc_keys FORMAT Null SETTINGS log_comment = '05238_c1';

SYSTEM CLEAR MARK CACHE;
SELECT countIf(mapContains(m, materialize('hot'))) FROM t_wkc_keys FORMAT Null SETTINGS log_comment = '05238_nonconst_contains';

SYSTEM FLUSH LOGS query_log;

SELECT 'map_keys_fewer_files_than_full',
    (
        SELECT ProfileEvents['FileOpen']
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND log_comment = '05238_map_keys'
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
            AND log_comment = '05238_full'
            AND event_date >= yesterday()
            AND event_time >= now() - 600
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

SELECT 'map_keys_less_bytes_than_cold_value',
    4 *
    (
        SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes']
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND log_comment = '05238_map_keys'
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
            AND log_comment = '05238_c1'
            AND event_date >= yesterday()
            AND event_time >= now() - 600
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

SELECT 'nonconst_contains_less_bytes_than_full',
    4 *
    (
        SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes']
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND log_comment = '05238_nonconst_contains'
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
            AND log_comment = '05238_full'
            AND event_date >= yesterday()
            AND event_time >= now() - 600
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

DROP TABLE t_wkc_keys;
