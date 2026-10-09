-- Test: ABR engine behavior with tables having the same name in different databases
-- This test verifies which tables become the underlying tables for ABR engine
-- when there are identically named tables across multiple databases

SET session_timezone = 'UTC';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier};

-- Create tables with the same name in both databases but different sample intervals

-- Database 1: Tables with sample intervals 1 and 10
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.different_db_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.different_db_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Database 2: Tables with the same base name but different sample intervals
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.different_db_1 (
    _sample_interval UInt64 DEFAULT 5,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.different_db_10 (
    _sample_interval UInt64 DEFAULT 60,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Insert test data into all tables
-- Database 1 tables
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.different_db_1
SELECT 1, toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR, 100 + number
FROM system.numbers LIMIT 50;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.different_db_10
SELECT 10, toDateTime('2025-11-11 12:00:00') - INTERVAL number DAY, 200 + number
FROM system.numbers LIMIT 10;

-- Database 2 tables
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.different_db_1
SELECT 5, toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR, 300 + number
FROM system.numbers LIMIT 50;

INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.different_db_10
SELECT 60, toDateTime('2025-11-11 12:00:00') - INTERVAL number DAY, 400 + number
FROM system.numbers LIMIT 10;

-- Test 1: Create ABR table in database 1 - should only use tables from database 1
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.abr ENGINE = ABR('different_db', [1, 10], ('ts'));

-- Query to verify which tables are being used (should be from db1 only)
SELECT /* test 05300, query 1 */ count(), max(value) AS max_val FROM {CLICKHOUSE_DATABASE_1:Identifier}.abr
WHERE ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 2 DAY;

-- Test 2: Create ABR table in database 2 - should only use tables from database 2
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.abr ENGINE = ABR('different_db', [1, 10], ('ts'));

-- Query to verify which tables are being used (should be from db2 only)
SELECT /* test 05300, query 2 */ count(), max(value) AS max_val FROM {CLICKHOUSE_DATABASE_2:Identifier}.abr
WHERE ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 2 DAY;

DROP TABLE IF EXISTS abr_nonexistent;
CREATE TABLE abr_nonexistent ENGINE = ABR('different_db', [1, 10], ('ts')); -- {serverError UNKNOWN_TABLE}

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.abr;
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier}.abr;
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.different_db_1;
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.different_db_10;
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier}.different_db_1;
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier}.different_db_10;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
