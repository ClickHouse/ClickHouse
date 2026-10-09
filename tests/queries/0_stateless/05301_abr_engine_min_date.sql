-- Test: ABR engine minimum date extraction from WHERE clauses
-- Verifies that ABR correctly extracts the minimum query date from various
-- filter expressions including AND, OR, NOT, and mixed conditions.

SET session_timezone = 'UTC';
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS abr_table_min_date;
DROP TABLE IF EXISTS abr_table_min_date_date;
DROP TABLE IF EXISTS abr_table_min_date_date32;
DROP TABLE IF EXISTS abr_table_min_date_datetime64;
DROP TABLE IF EXISTS abr_table_min_date_datetime64_subsecond;
DROP TABLE IF EXISTS abr_table_min_date_low_cardinality;
DROP TABLE IF EXISTS underlying_table_min_date_1;
DROP TABLE IF EXISTS underlying_table_min_date_10;
DROP TABLE IF EXISTS underlying_table_min_date_100;
DROP TABLE IF EXISTS underlying_table_min_date_date_1;
DROP TABLE IF EXISTS underlying_table_min_date_date32_1;
DROP TABLE IF EXISTS underlying_table_min_date_datetime64_1;
DROP TABLE IF EXISTS underlying_table_min_date_datetime64_10;
DROP TABLE IF EXISTS underlying_table_min_date_datetime64_subsecond_1;
DROP TABLE IF EXISTS underlying_table_min_date_datetime64_subsecond_10;
DROP TABLE IF EXISTS underlying_table_min_date_low_cardinality_1;

-- Table 1: 1 week retention
CREATE TABLE underlying_table_min_date_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Table 2: 1 month retention
CREATE TABLE underlying_table_min_date_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Table 3: 1 year retention
CREATE TABLE underlying_table_min_date_100 (
    _sample_interval UInt64 DEFAULT 100,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

CREATE TABLE underlying_table_min_date_date_1 (
    _sample_interval UInt64 DEFAULT 1,
    d Date,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(d)
ORDER BY d;

CREATE TABLE underlying_table_min_date_date32_1 (
    _sample_interval UInt64 DEFAULT 1,
    d Date32,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(d)
ORDER BY d;

CREATE TABLE underlying_table_min_date_datetime64_1 (
    _sample_interval UInt64 DEFAULT 1,
    d DateTime64(9),
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(d)
ORDER BY d;

CREATE TABLE underlying_table_min_date_datetime64_10 (
    _sample_interval UInt64 DEFAULT 10,
    d DateTime64(9),
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(d)
ORDER BY d;

CREATE TABLE underlying_table_min_date_datetime64_subsecond_1 (
    _sample_interval UInt64 DEFAULT 1,
    d DateTime64(9),
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(d)
ORDER BY d;

CREATE TABLE underlying_table_min_date_datetime64_subsecond_10 (
    _sample_interval UInt64 DEFAULT 10,
    d DateTime64(9),
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(d)
ORDER BY d;

CREATE TABLE underlying_table_min_date_low_cardinality_1 (
    _sample_interval UInt64 DEFAULT 1,
    d LowCardinality(Date32),
    value UInt64
) ENGINE = MergeTree()
PARTITION BY d
ORDER BY d;

CREATE TABLE abr_table_min_date
    ENGINE = ABR('underlying_table_min_date', [1, 10, 100], ('ts'));
CREATE TABLE abr_table_min_date_date
    ENGINE = ABR('underlying_table_min_date_date', [1], ('d'));
CREATE TABLE abr_table_min_date_date32
    ENGINE = ABR('underlying_table_min_date_date32', [1], ('d'));
CREATE TABLE abr_table_min_date_datetime64
    ENGINE = ABR('underlying_table_min_date_datetime64', [1, 10], ('d'));
CREATE TABLE abr_table_min_date_datetime64_subsecond
    ENGINE = ABR('underlying_table_min_date_datetime64_subsecond', [1, 10], ('d'));
CREATE TABLE abr_table_min_date_low_cardinality
    ENGINE = ABR('underlying_table_min_date_low_cardinality', [1], ('d'));

-- Insert 1 week of hourly data into underlying_table_min_date_1
INSERT INTO underlying_table_min_date_1
SELECT
    1,
    toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR,
    1
FROM system.numbers
LIMIT (24 * 7) + 1;

-- Insert 1 month of hourly data into underlying_table_min_date_10
INSERT INTO underlying_table_min_date_10
SELECT
    10,
    toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR,
    10
FROM system.numbers
LIMIT (24 * 31) + 1;

-- Insert 1 year of hourly data into underlying_table_min_date_100
INSERT INTO underlying_table_min_date_100
SELECT
    100,
    toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR,
    100
FROM system.numbers
LIMIT (24 * 365) + 1;

-- One timestamp filter
SELECT /* test 05301, query 1 */ count() FROM abr_table_min_date WHERE ts >= toDateTime('2025-11-11 12:00:00');
SELECT /* test 05301, query 2 */ count() FROM abr_table_min_date WHERE ts > toDateTime('2025-11-11 12:00:00');
SELECT /* test 05301, query 3 */ count() FROM abr_table_min_date WHERE ts <= toDateTime('2025-11-11 12:00:00'); -- {serverError QUERY_NOT_ALLOWED}
SELECT /* test 05301, query 4 */ count() FROM abr_table_min_date WHERE ts < toDateTime('2025-11-11 12:00:00'); -- {serverError QUERY_NOT_ALLOWED}
SELECT /* test 05301, query 5 */ count() FROM abr_table_min_date WHERE ts = toDateTime('2025-11-11 12:00:00');
SELECT /* test 05301, query 6 */ count() FROM abr_table_min_date WHERE ts != toDateTime('2025-11-11 12:00:00'); -- {serverError QUERY_NOT_ALLOWED}

SYSTEM FLUSH LOGS query_log;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 1 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 2 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 5 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Two timestamp filters
-- AND LOGIC (INTERSECTION)
SELECT /* test 05301, query 7 */ count() FROM abr_table_min_date WHERE ts >= toDateTime('2025-11-05 00:00:00') AND ts >= toDateTime('2025-11-01 00:00:00');
SELECT /* test 05301, query 8 */ count() FROM abr_table_min_date WHERE ts >= toDateTime('2025-11-01 00:00:00') AND ts < toDateTime('2025-11-05 00:00:00');
SELECT /* test 05301, query 9 */ count() FROM abr_table_min_date WHERE ts >= toDateTime('2025-11-05 00:00:00') AND ts < toDateTime('2025-11-01 00:00:00'); -- {serverError QUERY_NOT_ALLOWED}
SELECT /* test 05301, query 10 */ count() FROM abr_table_min_date WHERE ts = toDateTime('2025-11-05 00:00:00') AND ts = toDateTime('2025-11-01 00:00:00'); -- {serverError QUERY_NOT_ALLOWED}

SYSTEM FLUSH LOGS query_log;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 7 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 8 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;


-- Two timestamp filters
-- OR LOGIC (UNION)
SELECT /* test 05301, query 11 */ count() FROM abr_table_min_date WHERE ts >= toDateTime('2025-11-05 00:00:00') OR ts >= toDateTime('2025-11-01 00:00:00');
SELECT /* test 05301, query 12 */ count() FROM abr_table_min_date WHERE ts >= toDateTime('2025-11-05 00:00:00') OR ts < toDateTime('2025-11-01 00:00:00'); -- {serverError QUERY_NOT_ALLOWED}
SELECT /* test 05301, query 13 */ count() FROM abr_table_min_date WHERE ts >= toDateTime('2025-11-10 00:00:00') OR ts = toDateTime('2025-11-01 00:00:00');

SYSTEM FLUSH LOGS query_log;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 11 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 13 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;


-- ALL AND (Tightest Constraint)
SELECT /* test 05301, query 14 */ count() FROM abr_table_min_date WHERE ts >= toDateTime('2025-11-01 00:00:00') AND ts >= toDateTime('2025-11-05 00:00:00') AND ts >= toDateTime('2025-11-10 00:00:00');

-- ALL OR (Loosest Constraint)
SELECT /* test 05301, query 15 */ count() FROM abr_table_min_date WHERE ts >= toDateTime('2025-11-01 00:00:00') OR ts >= toDateTime('2025-11-05 00:00:00') OR ts >= toDateTime('2025-11-10 00:00:00');

-- MIXED: (A OR B) AND C
-- Logic: (Nov 1 OR Nov 5) -> Nov 1. Then: Nov 1 AND Nov 10 -> Nov 10.
SELECT /* test 05301, query 16 */ count() FROM abr_table_min_date WHERE (ts >= toDateTime('2025-11-01 00:00:00') OR ts >= toDateTime('2025-11-05 00:00:00')) AND ts >= toDateTime('2025-11-10 00:00:00');

-- MIXED: A OR (B AND C)
-- Logic: (Nov 5 AND Nov 10) -> Nov 10. Then: Nov 1 OR Nov 10 -> Nov 1.
SELECT /* test 05301, query 17 */ count() FROM abr_table_min_date WHERE ts >= toDateTime('2025-11-01 00:00:00') OR (ts >= toDateTime('2025-11-05 00:00:00') AND ts >= toDateTime('2025-11-10 00:00:00'));

-- SANDWICH WITH HOLE
-- Logic: Range is Nov 1 to Nov 10, excluding Nov 5. Lower bound remains Nov 1.
SELECT /* test 05301, query 18 */ count() FROM abr_table_min_date WHERE ts >= toDateTime('2025-11-01 00:00:00') AND ts <= toDateTime('2025-11-10 00:00:00') AND ts != toDateTime('2025-11-05 00:00:00');

-- DOUBLE NEGATIVE (De Morgan's)
-- Logic: NOT (ts < Nov 5 OR ts < Nov 1) is equivalent to (ts >= Nov 5 AND ts >= Nov 1). Result is Nov 5.
SELECT /* test 05301, query 19 */ count() FROM abr_table_min_date WHERE NOT (ts < toDateTime('2025-11-05 00:00:00') OR ts < toDateTime('2025-11-01 00:00:00'));

SYSTEM FLUSH LOGS query_log;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 14 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 15 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 16 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 17 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 18 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 19 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;


-- Verifies that ABR correctly extracts the minimum query date from various temporal types.
INSERT INTO underlying_table_min_date_date_1 VALUES (1, toDate('2025-11-11'), 1);
INSERT INTO underlying_table_min_date_date32_1 VALUES (1, toDate32('2025-11-11'), 1);
INSERT INTO underlying_table_min_date_datetime64_1 VALUES (1, toDateTime64('2025-11-05 00:00:00.000000000', 9, 'UTC'), 1);
INSERT INTO underlying_table_min_date_datetime64_10 VALUES
    (10, toDateTime64('2025-11-04 23:59:59.999999999', 9, 'UTC'), 10),
    (10, toDateTime64('2025-11-05 00:00:00.000000000', 9, 'UTC'), 10);

SELECT /* test 05301, query 20 */ count() FROM abr_table_min_date_date WHERE d >= toDate('2025-11-05');
SELECT /* test 05301, query 21 */ count() FROM abr_table_min_date_date32 WHERE d >= toDate32('2025-11-05');
SELECT /* test 05301, query 28 */ count() FROM abr_table_min_date_date WHERE d > toDate('2025-11-04');
SELECT /* test 05301, query 29 */ count() FROM abr_table_min_date_date32 WHERE d > toDate32('2025-11-04');
-- Preserve DateTime64 precision and exclusivity when choosing the underlying table.
SELECT /* test 05301, query 24 */ max(value) FROM abr_table_min_date_datetime64 WHERE d >= toDateTime64('2025-11-04 23:59:59.999999999', 9, 'UTC');
SELECT /* test 05301, query 25 */ max(value) FROM abr_table_min_date_datetime64 WHERE d > toDateTime64('2025-11-04 23:59:59.999999999', 9, 'UTC');
SELECT /* test 05301, query 26 */ max(value) FROM abr_table_min_date_datetime64 WHERE d >= toDateTime64('2025-11-04 23:59:59.999999999', 3, 'UTC');
SELECT /* test 05301, query 27 */ max(value) FROM abr_table_min_date_datetime64 WHERE d > toDateTime64('2025-11-04 23:59:59.999999999', 3, 'UTC');
-- Subsecond routing must not treat all values within the same second as equal.
INSERT INTO underlying_table_min_date_datetime64_subsecond_1 VALUES
    (1, toDateTime64('2025-11-05 00:00:00.900000000', 9, 'UTC'), 1);
INSERT INTO underlying_table_min_date_datetime64_subsecond_10 VALUES
    (10, toDateTime64('2025-11-05 00:00:00.800000000', 9, 'UTC'), 10),
    (10, toDateTime64('2025-11-05 00:00:00.900000000', 9, 'UTC'), 10);
SELECT max(value) FROM abr_table_min_date_datetime64_subsecond WHERE d >= toDateTime64('2025-11-05 00:00:00.800000000', 9, 'UTC');
SELECT max(value) FROM abr_table_min_date_datetime64_subsecond WHERE d > toDateTime64('2025-11-05 00:00:00.800000000', 9, 'UTC');
SELECT max(value) FROM abr_table_min_date_datetime64_subsecond WHERE d >= toDateTime64('2025-11-05 00:00:00.900000000', 9, 'UTC');
-- Advancing an exclusive bound past the maximum DateTime64 value must not overflow.
SELECT count() FROM abr_table_min_date_datetime64
WHERE d > toDateTime64('2262-04-11 23:47:16.854775807', 9, 'UTC'); -- {serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE}

SYSTEM FLUSH LOGS query_log;
SELECT abr_min_date as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 20 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT abr_min_date as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 21 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT abr_min_date as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 28 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT abr_min_date as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 29 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT abr_min_date as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 24 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT abr_min_date as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 25 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT abr_min_date as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 26 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT abr_min_date as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 27 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Wrapped Date and Date32 types also use epoch seconds.
INSERT INTO underlying_table_min_date_low_cardinality_1 VALUES (1, toDate32('2025-11-11'), 1);
INSERT INTO underlying_table_min_date_date32_1 VALUES (1, toDate32('1960-01-01'), 1);
INSERT INTO underlying_table_min_date_datetime64_1 VALUES (1, toDateTime64('1969-12-31 23:59:59.999999999', 9, 'UTC'), 1);

SELECT /* test 05301, query 22 */ count() FROM abr_table_min_date_low_cardinality WHERE d >= toDate32('2025-11-05');

SYSTEM FLUSH LOGS query_log;
SELECT abr_min_date as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05301, query 22 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Pre-epoch query bounds are not supported.
SELECT count() FROM abr_table_min_date_date32 WHERE d >= toDate32('1960-01-01'); -- {serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE}
SELECT count() FROM abr_table_min_date_datetime64 WHERE d >= toDateTime64('1969-12-31 23:59:59.999999999', 9, 'UTC'); -- {serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE}

-- Stored pre-epoch minima are treated as epoch because all supported query bounds are non-negative.
SELECT count() FROM abr_table_min_date_date32 WHERE d >= toDate32('2025-11-05');
SELECT count() FROM abr_table_min_date_datetime64 WHERE d >= toDateTime64('2025-11-05 00:00:00', 9, 'UTC');

DROP TABLE IF EXISTS abr_table_min_date;
DROP TABLE IF EXISTS abr_table_min_date_date;
DROP TABLE IF EXISTS abr_table_min_date_date32;
DROP TABLE IF EXISTS abr_table_min_date_datetime64;
DROP TABLE IF EXISTS abr_table_min_date_datetime64_subsecond;
DROP TABLE IF EXISTS abr_table_min_date_low_cardinality;
DROP TABLE IF EXISTS underlying_table_min_date_1;
DROP TABLE IF EXISTS underlying_table_min_date_10;
DROP TABLE IF EXISTS underlying_table_min_date_100;
DROP TABLE IF EXISTS underlying_table_min_date_date_1;
DROP TABLE IF EXISTS underlying_table_min_date_date32_1;
DROP TABLE IF EXISTS underlying_table_min_date_datetime64_1;
DROP TABLE IF EXISTS underlying_table_min_date_datetime64_10;
DROP TABLE IF EXISTS underlying_table_min_date_datetime64_subsecond_1;
DROP TABLE IF EXISTS underlying_table_min_date_datetime64_subsecond_10;
DROP TABLE IF EXISTS underlying_table_min_date_low_cardinality_1;
