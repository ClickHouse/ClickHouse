-- Tags: no-old-analyzer

-- `spilled_to_disk` names the operators that wrote data to temporary files, so an operator that only
-- created the files without ever writing to them is not one of them. `GraceHashJoin` allocates the
-- temporary buffers of every bucket as soon as it starts, and with a single bucket it then joins
-- everything in memory, so the files exist and stay empty. That query must report an empty array,
-- while the same join over enough buckets to be flushed must report `join`.

SET log_queries = 1;

SELECT 'the buckets are created but never written to';
SELECT count() FROM (SELECT number AS a FROM numbers(10000)) g1 JOIN (SELECT number AS a FROM numbers(10000)) g2 ON g1.a = g2.a
FORMAT Null
SETTINGS log_comment = '05046_spill_a_grace_in_memory', join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 1, grace_hash_join_max_buckets = 1;

SELECT 'the buckets are written to';
SELECT count() FROM (SELECT number AS a FROM numbers(10000)) g1 JOIN (SELECT number AS a FROM numbers(10000)) g2 ON g1.a = g2.a
FORMAT Null
SETTINGS log_comment = '05046_spill_b_grace_flushed', join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 32, grace_hash_join_max_buckets = 32;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, spilled_to_disk,
       ProfileEvents['ExternalJoinWritePart'] > 0 AS temporary_files_were_created,
       ProfileEvents['ExternalJoinCompressedBytes'] > 0 AS bytes_were_written
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05046\_spill\_%'
ORDER BY log_comment;
