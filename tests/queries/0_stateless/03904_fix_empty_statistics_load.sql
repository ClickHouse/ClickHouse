-- Test for issue #96068

SET use_statistics = 1;

DROP TABLE IF EXISTS tab;

-- The table has no manually or automatically created statistics
CREATE TABLE tab
(
    u64                 UInt64,
    u64_tdigest         UInt64,
    u64_minmax          UInt64,
    u64_countmin        UInt64,
    f64                 Float64,
    b                   Bool,
    s                   String,
) Engine = MergeTree() ORDER BY tuple() PARTITION BY u64_minmax
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

-- Insert looooots of parts (1000)
INSERT INTO tab
SELECT number % 1000,
       number % 1000,
       number % 99,
       number % 1000,
       number % 1000,
       number % 2,
       toString(number % 1000)
FROM system.numbers LIMIT 10000;

SELECT * FROM tab
WHERE u64_countmin > 3500 and u64_countmin < 3600
FORMAT NULL
SETTINGS use_statistics_cache = 0, log_comment = '03904_empty';

SYSTEM FLUSH LOGS query_log;

-- Expect that no statistics were loaded from disk
SELECT ProfileEvents['LoadedStatisticsMicroseconds']
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03904_empty'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE IF EXISTS tab;
