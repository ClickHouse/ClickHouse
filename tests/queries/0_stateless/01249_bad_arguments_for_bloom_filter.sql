-- Tags: no-parallel

SET send_logs_level = 'fatal';

DROP DATABASE IF EXISTS test_01249;
set allow_deprecated_database_ordinary=1;
-- Creation of a database with Ordinary engine emits a warning.
CREATE DATABASE test_01249 ENGINE=Ordinary;     -- Full ATTACH requires UUID with Atomic
USE test_01249;

CREATE TABLE bloom_filter_idx_good(`u64` UInt64, `i32` Int32, `f64` Float64, `d` Decimal(10, 2), `s` String, `e` Enum8('a' = 1, 'b' = 2, 'c' = 3), `dt` Date, INDEX bloom_filter_a i32 TYPE bloom_filter(0, 1) GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE bloom_filter_idx_good(`u64` UInt64, `i32` Int32, `f64` Float64, `d` Decimal(10, 2), `s` String, `e` Enum8('a' = 1, 'b' = 2, 'c' = 3), `dt` Date, INDEX bloom_filter_a i32 TYPE bloom_filter(-0.1) GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bloom_filter_idx_good(`u64` UInt64, `i32` Int32, `f64` Float64, `d` Decimal(10, 2), `s` String, `e` Enum8('a' = 1, 'b' = 2, 'c' = 3), `dt` Date, INDEX bloom_filter_a i32 TYPE bloom_filter(1.01) GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS bloom_filter_idx_good;
ATTACH TABLE bloom_filter_idx_good(`u64` UInt64, `i32` Int32, `f64` Float64, `d` Decimal(10, 2), `s` String, `e` Enum8('a' = 1, 'b' = 2, 'c' = 3), `dt` Date, INDEX bloom_filter_a i32 TYPE bloom_filter(0., 1.) GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192;
SHOW CREATE TABLE bloom_filter_idx_good;

DROP TABLE IF EXISTS bloom_filter_idx_good;
ATTACH TABLE bloom_filter_idx_good(`u64` UInt64, `i32` Int32, `f64` Float64, `d` Decimal(10, 2), `s` String, `e` Enum8('a' = 1, 'b' = 2, 'c' = 3), `dt` Date, INDEX bloom_filter_a i32 TYPE bloom_filter(-0.1) GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192;
SHOW CREATE TABLE bloom_filter_idx_good;

DROP TABLE IF EXISTS bloom_filter_idx_good;
ATTACH TABLE bloom_filter_idx_good(`u64` UInt64, `i32` Int32, `f64` Float64, `d` Decimal(10, 2), `s` String, `e` Enum8('a' = 1, 'b' = 2, 'c' = 3), `dt` Date, INDEX bloom_filter_a i32 TYPE bloom_filter(1.01) GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192;
SHOW CREATE TABLE bloom_filter_idx_good;

DROP DATABASE test_01249;
