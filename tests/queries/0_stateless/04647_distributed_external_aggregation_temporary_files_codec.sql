-- Tags: long, distributed, no-flaky-check
-- The temporary data settings of a spilling `GROUP BY` must survive query plan serialization,
-- so that a remote shard spills with the initiator's `temporary_files_codec` and under the
-- initiator's `allow_experimental_codecs` opt-in, instead of the shard's own defaults.

SET serialize_query_plan = 1;
SET prefer_localhost_replica = 0;
SET max_bytes_before_external_group_by = '100K';
SET max_bytes_ratio_before_external_group_by = 0;
SET group_by_two_level_threshold = '100K';
SET group_by_two_level_threshold_bytes = '50M';
SET max_memory_usage = '2G';

CREATE TEMPORARY TABLE start_ts AS ( SELECT now() AS ts );

-- Without the opt-in, the shard must reject the experimental codec, exactly like a local spill would.
SELECT number, count() FROM remote('127.0.0.{1,2}', numbers(2_000_000)) GROUP BY number
SETTINGS temporary_files_codec = 'ZXC'
FORMAT Null; -- { serverError BAD_ARGUMENTS }

-- With the opt-in, the shard spills with the codec chosen by the initiator.
SELECT number, count() FROM remote('127.0.0.{1,2}', numbers(2_000_000)) GROUP BY number
SETTINGS log_comment = '04647_distributed_external_aggregation_temporary_files_codec', allow_experimental_codecs = 1, temporary_files_codec = 'ZXC'
FORMAT Null;

SYSTEM FLUSH LOGS system.query_log;

-- The spill has to happen on the shard (`is_initial_query = 0`), otherwise the test would pass
-- without exercising the deserialized plan at all.
SELECT sum(ProfileEvents['ExternalProcessingCompressedBytesTotal']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= (SELECT ts FROM start_ts)
    AND current_database = currentDatabase()
    AND type != 1
    AND is_initial_query = 0
    AND log_comment = '04647_distributed_external_aggregation_temporary_files_codec';
