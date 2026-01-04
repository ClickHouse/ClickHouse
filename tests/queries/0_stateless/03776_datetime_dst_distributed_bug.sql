-- Tags: shard
-- Issue: https://github.com/ClickHouse/ClickHouse/issues/92208

SET enable_analyzer = 1;

DROP TABLE IF EXISTS ts_data_dst_03776;

-- 2025-10-26 03:00 CEST -> 02:00 CET because of DST
CREATE TABLE ts_data_dst_03776 (
    ts UInt64,
    dt DateTime64(6, 'Europe/Prague') DEFAULT fromUnixTimestamp64Micro(ts, 'Europe/Prague')
) ENGINE = MergeTree ORDER BY dt;

-- 1761429601000000 = 2025-10-26 00:00:01 CEST (UTC+2)
-- 1761433201000000 = 2025-10-26 01:00:01 CEST (UTC+2)
-- 1761436801000000 = 2025-10-26 02:00:01 CEST (UTC+2) - first 02:00 (before DST change)
-- 1761440401000000 = 2025-10-26 02:00:01 CET (UTC+1) - second 02:00 (after DST change)
-- 1761444001000000 = 2025-10-26 03:00:01 CET (UTC+1)

INSERT INTO ts_data_dst_03776 (ts) VALUES (1761429601000000), (1761433201000000), (1761436801000000), (1761440401000000), (1761444001000000);

SELECT '-- Test 1: DateTime64 local query';
WITH
    toDateTime64('2025-10-26 00:00:00', 6, 'Europe/Prague') AS min_dt,
    (min_dt + INTERVAL 3 HOUR) AS max_dt
SELECT count() FROM ts_data_dst_03776 WHERE dt > min_dt AND dt < max_dt;

-- should return the same count as the local query
SELECT '-- Test 2: DateTime64 distributed query via remote()';
WITH
    toDateTime64('2025-10-26 00:00:00', 6, 'Europe/Prague') AS min_dt,
    (min_dt + INTERVAL 3 HOUR) AS max_dt
SELECT count() FROM remote('127.0.0.1', currentDatabase(), ts_data_dst_03776) 
WHERE dt > min_dt AND dt < max_dt
SETTINGS prefer_localhost_replica = 0;

-- verify the exact timestamp at 02:00:01 CET (after DST)
SELECT '-- Test 3: DateTime64 exact match at post-DST time - local';
SELECT count() FROM ts_data_dst_03776 
WHERE dt = toDateTime64(1761440401, 6, 'Europe/Prague');

-- same test but remote
SELECT '-- Test 4: DateTime64 exact match at post-DST time - remote';
SELECT count() FROM remote('127.0.0.1', currentDatabase(), ts_data_dst_03776) 
WHERE dt = toDateTime64(1761440401, 6, 'Europe/Prague')
SETTINGS prefer_localhost_replica = 0;

DROP TABLE IF EXISTS ts_data_dst_03776;

-- DateTime Tests

DROP TABLE IF EXISTS ts_data_dt_dst_03776;

CREATE TABLE ts_data_dt_dst_03776 (
    ts UInt32,
    dt DateTime('Europe/Prague') DEFAULT fromUnixTimestamp(ts)
) ENGINE = MergeTree ORDER BY dt;

-- inserts same stuff as for dt64

INSERT INTO ts_data_dt_dst_03776 (ts) VALUES (1761429601), (1761433201), (1761436801), (1761440401), (1761444001);

SELECT '-- Test 5: DateTime local query';
WITH
    toDateTime('2025-10-26 00:00:00', 'Europe/Prague') AS min_dt,
    (min_dt + INTERVAL 3 HOUR) AS max_dt
SELECT count() FROM ts_data_dt_dst_03776 WHERE dt > min_dt AND dt < max_dt;

SELECT '-- Test 6: DateTime distributed query via remote()';
WITH
    toDateTime('2025-10-26 00:00:00', 'Europe/Prague') AS min_dt,
    (min_dt + INTERVAL 3 HOUR) AS max_dt
SELECT count() FROM remote('127.0.0.1', currentDatabase(), ts_data_dt_dst_03776) 
WHERE dt > min_dt AND dt < max_dt
SETTINGS prefer_localhost_replica = 0;

SELECT '-- Test 7: DateTime exact match at post-DST time - local';
SELECT count() FROM ts_data_dt_dst_03776 
WHERE dt = toDateTime(1761440401, 'Europe/Prague');

SELECT '-- Test 8: DateTime exact match at post-DST time - remote';
SELECT count() FROM remote('127.0.0.1', currentDatabase(), ts_data_dt_dst_03776) 
WHERE dt = toDateTime(1761440401, 'Europe/Prague')
SETTINGS prefer_localhost_replica = 0;

SELECT '-- Test 9: All DateTime rows count via remote';
SELECT count() FROM remote('127.0.0.1', currentDatabase(), ts_data_dt_dst_03776)
SETTINGS prefer_localhost_replica = 0;

DROP TABLE IF EXISTS ts_data_dt_dst_03776;
