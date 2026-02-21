-- Tags: no-random-settings

-- Test: data_type_default_nullable_if_not_in_keys excludes ORDER BY columns from Nullable wrapping.

SET data_type_default_nullable = 1;
SET data_type_default_nullable_if_not_in_keys = 1;

-- Basic case: ORDER BY column stays non-nullable, others become Nullable.
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (id UInt64, name String, value Float64) ENGINE = MergeTree ORDER BY id;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't1' ORDER BY name;
DROP TABLE t1;

-- Compound ORDER BY: all key columns excluded.
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (id UInt64, date Date, name String) ENGINE = MergeTree ORDER BY (id, date);
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't2' ORDER BY name;
DROP TABLE t2;

-- PRIMARY KEY columns excluded too.
DROP TABLE IF EXISTS t3;
CREATE TABLE t3 (id UInt64, date Date, name String) ENGINE = MergeTree ORDER BY (id, date) PRIMARY KEY id;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't3' ORDER BY name;
DROP TABLE t3;

-- Compound expressions in ORDER BY: identifiers inside expressions are excluded.
DROP TABLE IF EXISTS t4;
CREATE TABLE t4 (id UInt64, date Date, name String) ENGINE = MergeTree ORDER BY (toStartOfMonth(date), id);
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't4' ORDER BY name;
DROP TABLE t4;

-- Explicit NULL/NOT NULL modifiers take precedence.
DROP TABLE IF EXISTS t5;
CREATE TABLE t5 (id UInt64 NULL, name String NOT NULL, value Float64) ENGINE = MergeTree ORDER BY name;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't5' ORDER BY name;
DROP TABLE t5;

-- Non-MergeTree (Memory): no ORDER BY, all columns become Nullable.
DROP TABLE IF EXISTS t6;
CREATE TABLE t6 (id UInt64, name String) ENGINE = Memory;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't6' ORDER BY name;
DROP TABLE t6;

-- Settings off: original behavior (all columns Nullable).
SET data_type_default_nullable_if_not_in_keys = 0;
DROP TABLE IF EXISTS t7;
CREATE TABLE t7 (id UInt64, name String) ENGINE = MergeTree ORDER BY id SETTINGS allow_nullable_key = 1;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't7' ORDER BY name;
DROP TABLE t7;
