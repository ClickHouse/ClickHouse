-- Tags: long

-- External `DISTINCT` resolves `temporary_files_codec` at the first spill, long after the query
-- settings are gone, so `DistinctStep` has to carry the session's authorization of a gated codec
-- along with the codec string - exactly like the external sort, the external aggregation and the
-- grace hash join do. Covered here with `ZXC`, the generic experimental test codec, so the test
-- does not depend on the Rust part of the build.

SET max_bytes_before_external_distinct = 1;
SET max_bytes_ratio_before_external_distinct = 0;
SET max_memory_usage = '1G';
-- The deduplication has to go through the hashing `DISTINCT`: on a stream already sorted by the
-- `DISTINCT` key the pipeline uses `DistinctSortedStreamTransform`, which holds one range of equal
-- values at a time and never touches the temporary data.
SET optimize_distinct_in_order = 0;

CREATE TEMPORARY TABLE start_ts AS ( SELECT now() AS ts );

-- Without the opt-in, the gated codec is rejected at the first spill.
SELECT DISTINCT intHash64(number) FROM numbers(1_000_000)
SETTINGS temporary_files_codec = 'ZXC'
FORMAT Null; -- { serverError BAD_ARGUMENTS }

-- `enable_zxc_codec` authorizes it, and the authorization has to survive all the way into the spill.
SELECT DISTINCT intHash64(number) FROM numbers(1_000_000)
SETTINGS log_comment = '05230_external_distinct_temporary_files_codec_gate/distinct', enable_zxc_codec = 1, temporary_files_codec = 'ZXC'
FORMAT Null;

-- A different codec's dedicated setting does not authorize `ZXC`.
SELECT DISTINCT intHash64(number) FROM numbers(1_000_000)
SETTINGS enable_alp_codec = 1, temporary_files_codec = 'ZXC'
FORMAT Null; -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS system.query_log;

-- Make sure the successful queries actually spilled compressed data, so that the test cannot pass
-- without having resolved the codec.
SELECT log_comment, sum(ProfileEvents['ExternalProcessingCompressedBytesTotal']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= (SELECT ts FROM start_ts)
    AND current_database = currentDatabase()
    AND type != 1
    AND log_comment = '05230_external_distinct_temporary_files_codec_gate/distinct'
GROUP BY log_comment
ORDER BY log_comment;
