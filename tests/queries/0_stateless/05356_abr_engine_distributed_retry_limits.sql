-- Tags: shard
-- The read limits of an `ABR` query are split between its attempts, and an attempt that fails with `TOO_MANY_ROWS`
-- is retried on a coarser table. The estimate of the rows the failed attempt planned to read must stay with that
-- attempt: read through a `Distributed` table, the initiator checks the limits of the whole query against the progress
-- the shard reports, and an estimate that was reported cannot be withdrawn.

SET allow_experimental_abr = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS dist_abr;
DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS retry_limits_1;
DROP TABLE IF EXISTS retry_limits_10;

-- Keep each part in one granule so the read-limit checks do not depend on randomized granularity.
CREATE TABLE retry_limits_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
CREATE TABLE retry_limits_10 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

-- 500 hourly rows, of which 487 in the December 2025 part, and 500 rows every 10 hours.
INSERT INTO retry_limits_1 SELECT toDateTime('2026-01-01 12:00:00') - INTERVAL number HOUR, 1 FROM numbers(500);
INSERT INTO retry_limits_10 SELECT toDateTime('2026-01-01 12:00:00') - INTERVAL number * 10 HOUR, 10 FROM numbers(500);

CREATE TABLE abr ENGINE = ABR('retry_limits', [1, 10], ('ts'));
CREATE TABLE dist_abr AS abr ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'abr');

-- Each attempt may read 225 rows. With the implicit projections, the attempt on interval 1 is not rejected when it is
-- planned: it reports an estimate of 487 rows and fails with `TOO_MANY_ROWS` after reading a few. The attempt on
-- interval 10 reads 46 rows, so the query is well within the limit of 450 rows.
SELECT count(), max(value) FROM dist_abr
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 450 HOUR
SETTINGS
    max_rows_to_read = 450,
    optimize_use_implicit_projections = 1,
    max_block_size = 1,
    max_threads = 1,
    prefer_localhost_replica = 0,
    send_logs_level = 'error',
    log_comment = '05356_distributed_retry';

-- The query on the shard was retried on interval 10 after the attempt on interval 1 failed with `TOO_MANY_ROWS`.
-- The shard runs in the default database of its connection, so its entry is found through the initial query.
SYSTEM FLUSH LOGS query_log;
SELECT abr_interval, length(abr_exception_codes), errorCodeToName(abr_exception_codes[1])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND NOT is_initial_query
    AND event_date >= yesterday()
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND is_initial_query
            AND log_comment = '05356_distributed_retry'
            AND event_date >= yesterday());

-- An attempt that exceeds its share of `max_bytes_to_read_leaf`, which only applies on the shard, is retried too. Each
-- attempt may read 3000 bytes. The attempt on interval 1 reads `ts` (4 bytes) and `value` (8 bytes) of at least the 451
-- matching rows, so at least 5412 bytes, and fails with `TOO_MANY_BYTES`. The attempt on interval 10 reads less than
-- 1000 bytes from the parts of December and January.
SELECT count(), max(value) FROM dist_abr
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 450 HOUR
SETTINGS
    max_bytes_to_read_leaf = 6000,
    prefer_localhost_replica = 0,
    send_logs_level = 'error',
    log_comment = '05356_distributed_retry_bytes_leaf';

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval, length(abr_exception_codes), errorCodeToName(abr_exception_codes[1])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND NOT is_initial_query
    AND event_date >= yesterday()
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND is_initial_query
            AND log_comment = '05356_distributed_retry_bytes_leaf'
            AND event_date >= yesterday());

DROP TABLE dist_abr;
DROP TABLE abr;
DROP TABLE retry_limits_1;
DROP TABLE retry_limits_10;
