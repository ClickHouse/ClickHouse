-- Tags: long
SET max_bytes_before_external_sort = '1M';
SET max_bytes_ratio_before_external_sort = 0;
SET max_block_size = DEFAULT;
SET max_bytes_before_external_group_by = '1M';
SET max_bytes_ratio_before_external_group_by = 0;
SET group_by_two_level_threshold = '100K';
SET group_by_two_level_threshold_bytes = '50M';
SET max_memory_usage = '1G';

CREATE TEMPORARY TABLE start_ts AS ( SELECT now() AS ts );

SELECT * FROM (SELECT number, 'payload' FROM numbers(2_000_000)) ORDER BY number
SETTINGS log_comment='03772_temporary_files_codec/sort', temporary_files_codec = 'NONE'
FORMAT Null;

SELECT * FROM (SELECT number, 'payload' FROM numbers(2_000_000)) ORDER BY number
SETTINGS log_comment='03772_temporary_files_codec/sort', temporary_files_codec = 'LZ4'
FORMAT Null;

SELECT key, sum(val) FROM (SELECT number AS key, number as val FROM numbers(2_000_000)) GROUP BY key
SETTINGS log_comment='03772_temporary_files_codec/agg', temporary_files_codec = 'NONE'
FORMAT Null;

SELECT key, sum(val) FROM (SELECT number AS key, number as val FROM numbers(2_000_000)) GROUP BY key
SETTINGS log_comment='03772_temporary_files_codec/agg', temporary_files_codec = 'LZ4'
FORMAT Null;

SELECT key, sum(val) FROM (SELECT number AS key, number as val FROM numbers(2_000_000)) GROUP BY key
SETTINGS log_comment='03772_temporary_files_codec/agg', temporary_files_codec = 'NONE'
FORMAT Null;

SET max_bytes_in_join = '1M';
SET join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 32, grace_hash_join_max_buckets = 32;

SELECT * FROM (SELECT number AS key, number as val FROM numbers(200_000)) t1
JOIN (SELECT number AS key FROM numbers(200_000)) t2
USING key
SETTINGS log_comment='03772_temporary_files_codec/grace_join', temporary_files_codec = 'NONE'
FORMAT Null;

SELECT * FROM (SELECT number AS key, number as val FROM numbers(200_000)) t1
JOIN (SELECT number AS key FROM numbers(200_000)) t2
USING key
SETTINGS log_comment='03772_temporary_files_codec/grace_join', temporary_files_codec = 'LZ4'
FORMAT Null;

SET join_algorithm = 'partial_merge';

SELECT * FROM (SELECT number AS key, number as val FROM numbers(200_000)) t1
JOIN (SELECT number AS key FROM numbers(200_000)) t2
USING key
SETTINGS log_comment='03772_temporary_files_codec/partial_merge_join', temporary_files_codec = 'NONE'
FORMAT Null;

SELECT * FROM (SELECT number AS key, number as val FROM numbers(200_000)) t1
JOIN (SELECT number AS key FROM numbers(200_000)) t2
USING key
SETTINGS log_comment='03772_temporary_files_codec/partial_merge_join', temporary_files_codec = 'LZ4'
FORMAT Null;

SYSTEM FLUSH LOGS system.query_log;

SELECT
    log_comment,
    (sumIf(ProfileEvents['ExternalProcessingCompressedBytesTotal'], Settings['temporary_files_codec'] = 'LZ4') AS with_compression) > 0,
    (sumIf(ProfileEvents['ExternalProcessingUncompressedBytesTotal'], Settings['temporary_files_codec'] = 'NONE') AS without_compression) > 0,
    with_compression < without_compression
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= (SELECT ts FROM start_ts)
    AND current_database = currentDatabase()
    AND type != 1
    AND log_comment like '03772_temporary_files_codec/%'
GROUP BY log_comment
ORDER BY log_comment
;

