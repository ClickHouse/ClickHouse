SET max_threads = 4;
SET max_block_size = 1024;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 128;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 10000000;
SET group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;

-- Explicitly wide keys retain a hash-table method eligible for adaptive aggregation.
-- Empty and never-frozen producers close their admission streams without a staged payload.
-- Filtering at runtime keeps multiple source streams even though none produces a grouped row.
SELECT 'empty', count()
FROM (SELECT number AS k, count() FROM numbers_mt(8192) WHERE cityHash64(number) = 0 GROUP BY k);
SELECT 'never frozen', count(), sum(c)
FROM (SELECT toUInt64(number % 8) AS k, count() AS c FROM numbers_mt(8192) GROUP BY k)
SETTINGS adaptive_aggregator_freeze_threshold = 1000000;

-- Small input blocks leave candidates in the converter until final flushing. Count, general,
-- and key-only payloads must all reach admission before the final merge starts.
SELECT 'count final flush', count(), sum(c)
FROM (SELECT toUInt64(number % 16000) AS k, count() AS c FROM numbers_mt(64000) GROUP BY k);
SELECT 'general final flush', count(), sum(c), sum(s), sum(mx - mn)
FROM
(
    SELECT toUInt64(number % 16000) AS k, count() AS c, sum(number) AS s, min(number) AS mn, max(number) AS mx
    FROM numbers_mt(64000) GROUP BY k
);
SELECT 'key-only final flush', count(), sum(k)
FROM (SELECT toUInt64(number % 16000) AS k FROM numbers_mt(64000) GROUP BY k);

-- A downstream limit can close the result stream while bucket workers still retain staged keys.
SELECT 'limited result', count()
FROM (SELECT number AS k, max(toString(number)) FROM numbers_mt(200000) GROUP BY k LIMIT 1);
