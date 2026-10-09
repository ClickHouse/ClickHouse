-- Test: ABR engine fallback prefers finest table on equal retentions
-- When all underlying tables have effectively equal retention (within 1 day) and
-- the query's lower bound is older than all of them, chooseBestTable's fallback
-- must route to the FINEST table (highest quality), not the coarsest. This
-- simulates the "newly added shard" scenario where coarser tables have not yet
-- accumulated extra history.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr_table;
DROP TABLE IF EXISTS underlying_table_1;
DROP TABLE IF EXISTS underlying_table_10;
DROP TABLE IF EXISTS underlying_table_100;

CREATE TABLE underlying_table_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/underlying_table_1_eqret', '1')
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

CREATE TABLE underlying_table_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/underlying_table_10_eqret', '1')
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

CREATE TABLE underlying_table_100 (
    _sample_interval UInt64 DEFAULT 100,
    ts DateTime,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/underlying_table_100_eqret', '1')
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Equal retentions: all three tables hold the exact same 2 days of hourly data.
-- The `value` column doubles as a routing marker (1 / 10 / 100) so the SELECT's
-- max(value) reveals which underlying table answered the query.
INSERT INTO underlying_table_1
SELECT 1, toDateTime('2026-01-01 12:00:00') - INTERVAL number HOUR, 1
FROM system.numbers LIMIT (24 * 2) + 1;

INSERT INTO underlying_table_10
SELECT 10, toDateTime('2026-01-01 12:00:00') - INTERVAL number HOUR, 10
FROM system.numbers LIMIT (24 * 2) + 1;

INSERT INTO underlying_table_100
SELECT 100, toDateTime('2026-01-01 12:00:00') - INTERVAL number HOUR, 100
FROM system.numbers LIMIT (24 * 2) + 1;

CREATE TABLE abr_table
ENGINE = ABR('underlying_table', [1, 10, 100], ('ts'));

-- Q1: covered query (1 day back). All three tables cover; the cover loop wins
-- and returns the finest. Sanity check that the patch does not disturb the
-- cover path.
SELECT /* test 05311, query 1 */ count(), max(value) FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 1 DAY;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05311, query 1 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Q2: uncovered query (7 days back), the ticket scenario.
-- All three tables share an identical min_storage_date, so the cover loop finds
-- nothing and the fallback runs. Pre-fix: returns the coarsest (=100). Post-fix:
-- the day-precision tie is broken toward the finest (=1).
SELECT /* test 05311, query 2 */ count(), max(value) FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 7 DAY;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05311, query 2 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Q3: uncovered + abr_last_table_suffix=10 restricts the candidate range to
-- [1, 10]. Same equal-retention tie within the restricted range. Pre-fix:
-- returns end=10. Post-fix: tie -> finest=1.
SELECT /* test 05311, query 3 */ count(), max(value) FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 7 DAY
SETTINGS abr_last_table_suffix = 10;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05311, query 3 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Q4: uncovered + abr_first_table_suffix=10 restricts the candidate range to
-- [10, 100]. Same equal-retention tie within the restricted range. Pre-fix:
-- returns end=100. Post-fix: tie -> finest in range = 10.
SELECT /* test 05311, query 4 */ count(), max(value) FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 7 DAY
SETTINGS abr_first_table_suffix = 10;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05311, query 4 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

DROP TABLE IF EXISTS abr_table;
DROP TABLE IF EXISTS underlying_table_1;
DROP TABLE IF EXISTS underlying_table_10;
DROP TABLE IF EXISTS underlying_table_100;
