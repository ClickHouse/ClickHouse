-- Tags: long, no-fasttest, no-flaky-check
-- no-fasttest: the `PCO` codec needs the Rust part of the build

SET max_bytes_before_external_sort = '100K';
SET max_bytes_ratio_before_external_sort = 0;
SET max_block_size = DEFAULT;
SET max_bytes_before_external_group_by = '100K';
SET max_bytes_ratio_before_external_group_by = 0;
SET group_by_two_level_threshold = '100K';
SET group_by_two_level_threshold_bytes = '50M';
SET max_memory_usage = '1G';

CREATE TEMPORARY TABLE start_ts AS ( SELECT now() AS ts );

-- Without the opt-in, an experimental codec is rejected at the first spill.
SELECT * FROM (SELECT number, 'payload' FROM numbers(2_000_000)) ORDER BY number
SETTINGS temporary_files_codec = 'ZXC'
FORMAT Null; -- { serverError BAD_ARGUMENTS }

SELECT key, sum(val) FROM (SELECT number AS key, number AS val FROM numbers(2_000_000)) GROUP BY key
SETTINGS temporary_files_codec = 'ZXC'
FORMAT Null; -- { serverError BAD_ARGUMENTS }

-- `enable_zxc_codec` has to authorize the spill codec just like it authorizes a column codec: with
-- it, a generic experimental codec such as `ZXC` can compress spill files, and the opt-in travels
-- with the codec into the spill.
SELECT * FROM (SELECT number, 'payload' FROM numbers(2_000_000)) ORDER BY number
SETTINGS log_comment = '04646_temporary_files_codec_experimental_gate/sort', enable_zxc_codec = 1, temporary_files_codec = 'ZXC'
FORMAT Null;

SELECT key, sum(val) FROM (SELECT number AS key, number AS val FROM numbers(2_000_000)) GROUP BY key
SETTINGS log_comment = '04646_temporary_files_codec_experimental_gate/agg', enable_zxc_codec = 1, temporary_files_codec = 'ZXC'
FORMAT Null;

-- A different codec's dedicated setting does not authorize `ZXC`.
SELECT * FROM (SELECT number, 'payload' FROM numbers(2_000_000)) ORDER BY number
SETTINGS enable_pco_codec = 1, temporary_files_codec = 'ZXC'
FORMAT Null; -- { serverError BAD_ARGUMENTS }

SET max_bytes_in_join = '1M';
SET join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 32, grace_hash_join_max_buckets = 32;

SELECT * FROM (SELECT number AS key, number AS val FROM numbers(200_000)) t1
JOIN (SELECT number AS key FROM numbers(200_000)) t2
USING key
SETTINGS temporary_files_codec = 'ZXC'
FORMAT Null; -- { serverError BAD_ARGUMENTS }

SELECT * FROM (SELECT number AS key, number AS val FROM numbers(200_000)) t1
JOIN (SELECT number AS key FROM numbers(200_000)) t2
USING key
SETTINGS log_comment = '04646_temporary_files_codec_experimental_gate/grace_join', enable_zxc_codec = 1, temporary_files_codec = 'ZXC'
FORMAT Null;

-- The opt-in does not lift the column-type requirement: `PCO` cannot compress untyped spill data.
SELECT * FROM (SELECT number, 'payload' FROM numbers(2_000_000)) ORDER BY number
SETTINGS enable_pco_codec = 1, temporary_files_codec = 'PCO'
FORMAT Null; -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS system.query_log;

-- Make sure the successful `ZXC` queries actually spilled compressed data
-- (otherwise the test would pass trivially without exercising the codec).
SELECT log_comment, sum(ProfileEvents['ExternalProcessingCompressedBytesTotal']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= (SELECT ts FROM start_ts)
    AND current_database = currentDatabase()
    AND type != 1
    AND log_comment LIKE '04646_temporary_files_codec_experimental_gate/%'
GROUP BY log_comment
ORDER BY log_comment;
