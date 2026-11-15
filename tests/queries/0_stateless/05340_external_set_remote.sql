-- With a threshold of 1 byte, a set spills to disk before its first chunk. Without compression, the temporary
-- data of the set takes 8 bytes per key in the runs of its external sort and again in the finished set. Each
-- query reaches `remote` over the network, so its secondary query builds the set.
SET max_threads = 1;
SET max_block_size = 8192;
SET max_bytes_before_external_set = 1;
SET temporary_files_codec = 'NONE';
SET temporary_files_buffer_size = 65536;
SET prefer_localhost_replica = 0;
SET use_hedged_requests = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 1;
SET log_queries = 1;

-- The secondary query fills the set from the subquery of its serialized plan, and the temporary data of the
-- set counts toward the limit of that query. The runs of 262144 keys take 2 MiB.
SELECT * FROM remote('127.0.0.1', view(
    SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(262144))))
SETTINGS serialize_query_plan = 1, max_temporary_data_on_disk_size_for_query = 65536; -- { serverError TOO_MANY_ROWS_OR_BYTES }

-- The temporary data of 16384 keys takes 256 KiB and fits the limit.
SELECT 'serialized', * FROM remote('127.0.0.1', view(
    SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(16384))))
SETTINGS serialize_query_plan = 1, max_temporary_data_on_disk_size_for_query = 524288, log_comment = 'serialized';

-- With `GLOBAL IN`, the initiator sends the result of the subquery as a temporary table, and the secondary
-- query fills its set from that table, from the text of the query or from its serialized plan.
SELECT 'global text', count() FROM remote('127.0.0.1', numbers(30000))
WHERE number GLOBAL IN (SELECT number * 3 FROM numbers(16384))
SETTINGS serialize_query_plan = 0, log_comment = 'global text';
SELECT 'global serialized', count() FROM remote('127.0.0.1', numbers(30000))
WHERE number GLOBAL IN (SELECT number * 3 FROM numbers(16384))
SETTINGS serialize_query_plan = 1, log_comment = 'global serialized';

SYSTEM FLUSH LOGS query_log;

-- The secondary query of each case spills its set, writes temporary files and reads the set from them.
-- Secondary queries can run in another current database, so they are found through their initial queries.
SELECT log_comment, spilled_to_disk, ProfileEvents['SetsSpilledToDisk'], ProfileEvents['ExternalSetWritePart'] > 0,
    ProfileEvents['ExternalSetReadBlocks'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND NOT is_initial_query
    AND initial_query_id IN
    (
        SELECT query_id
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND is_initial_query
            AND current_database = currentDatabase() AND log_comment != ''
    )
ORDER BY event_time_microseconds;
