-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- Test for the columns cache feature.
-- The columns cache stores deserialized columns so that subsequent reads
-- of the same mark ranges do not need to decompress and deserialize again.

SET max_threads = 1;

DROP TABLE IF EXISTS t_columns_cache;

CREATE TABLE t_columns_cache (id UInt64, value String, extra UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;

INSERT INTO t_columns_cache SELECT number, toString(number), number * 10 FROM numbers(100000);

-- Baseline: query without columns cache
SELECT sum(id), count() FROM t_columns_cache
SETTINGS use_columns_cache = 0;

-- Enable columns cache: first read populates the cache
SELECT sum(id), count() FROM t_columns_cache
SETTINGS use_columns_cache = 1;

-- Second read should hit the columns cache and produce same result
SELECT sum(id), count() FROM t_columns_cache
SETTINGS use_columns_cache = 1;

-- Third read for consistency
SELECT sum(id), count() FROM t_columns_cache
SETTINGS use_columns_cache = 1;

-- Test with WHERE clause
SYSTEM DROP COLUMNS CACHE;

SELECT sum(id), count() FROM t_columns_cache WHERE id < 50000
SETTINGS use_columns_cache = 0;

SELECT sum(id), count() FROM t_columns_cache WHERE id < 50000
SETTINGS use_columns_cache = 1;

SELECT sum(id), count() FROM t_columns_cache WHERE id < 50000
SETTINGS use_columns_cache = 1;

-- Test with multiple columns
SYSTEM DROP COLUMNS CACHE;

SELECT sum(id), sum(extra), count() FROM t_columns_cache
SETTINGS use_columns_cache = 0;

SELECT sum(id), sum(extra), count() FROM t_columns_cache
SETTINGS use_columns_cache = 1;

SELECT sum(id), sum(extra), count() FROM t_columns_cache
SETTINGS use_columns_cache = 1;

-- Test with string column
SYSTEM DROP COLUMNS CACHE;

SELECT count(), sum(length(value)) FROM t_columns_cache WHERE id < 1000
SETTINGS use_columns_cache = 0;

SELECT count(), sum(length(value)) FROM t_columns_cache WHERE id < 1000
SETTINGS use_columns_cache = 1;

SELECT count(), sum(length(value)) FROM t_columns_cache WHERE id < 1000
SETTINGS use_columns_cache = 1;

-- Test with PREWHERE
SYSTEM DROP COLUMNS CACHE;

SELECT sum(id), count() FROM t_columns_cache PREWHERE extra < 500000 WHERE id < 50000
SETTINGS use_columns_cache = 0;

SELECT sum(id), count() FROM t_columns_cache PREWHERE extra < 500000 WHERE id < 50000
SETTINGS use_columns_cache = 1;

SELECT sum(id), count() FROM t_columns_cache PREWHERE extra < 500000 WHERE id < 50000
SETTINGS use_columns_cache = 1;

-- Test SYSTEM DROP COLUMNS CACHE
SYSTEM DROP COLUMNS CACHE;
SELECT 'SYSTEM DROP COLUMNS CACHE OK';

-- Test enable_writes_to_columns_cache = 0
SYSTEM DROP COLUMNS CACHE;
SELECT sum(id) FROM t_columns_cache WHERE id < 1000
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 0;

-- Reads disabled - should still produce correct result
SELECT sum(id) FROM t_columns_cache WHERE id < 1000
SETTINGS use_columns_cache = 1, enable_reads_from_columns_cache = 0;

-- Test with multiple parts
DROP TABLE IF EXISTS t_columns_cache_multi;

CREATE TABLE t_columns_cache_multi (id UInt64, value UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;

INSERT INTO t_columns_cache_multi SELECT number, number * 2 FROM numbers(10000);
INSERT INTO t_columns_cache_multi SELECT number + 10000, (number + 10000) * 2 FROM numbers(10000);
INSERT INTO t_columns_cache_multi SELECT number + 20000, (number + 20000) * 2 FROM numbers(10000);

SYSTEM DROP COLUMNS CACHE;

SELECT sum(id), sum(value), count() FROM t_columns_cache_multi
SETTINGS use_columns_cache = 0;

SELECT sum(id), sum(value), count() FROM t_columns_cache_multi
SETTINGS use_columns_cache = 1;

SELECT sum(id), sum(value), count() FROM t_columns_cache_multi
SETTINGS use_columns_cache = 1;

DROP TABLE t_columns_cache_multi;
DROP TABLE t_columns_cache;
