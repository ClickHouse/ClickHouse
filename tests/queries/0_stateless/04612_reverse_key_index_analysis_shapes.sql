-- Tags: no-random-merge-tree-settings

-- Primary key pruning on tables with reverse-sorted key columns must never drop granules that
-- contain matching rows. The shapes below are the tricky ones: a condition that bounds a `DESC`
-- column from below while another key column also participates, mark-range boundaries where an
-- earlier key column changes between marks, NULLs (stored physically first on a `DESC` column),
-- parts without a final mark (non-adaptive granularity), key columns not loaded in the in-memory
-- index, and key columns skipped by the sparse analysis. The core queries run through both index
-- analysis paths (`use_lightweight_primary_key_index_analysis` 1 and 0).
-- Scenarios ported from https://github.com/ClickHouse/ClickHouse/pull/111059.

SELECT 'equality on both key columns, DESC Enum second';
DROP TABLE IF EXISTS t_enum_rev;
CREATE TABLE t_enum_rev (g String, r Enum8('poor' = 1, 'ok' = 2, 'great' = 3))
ENGINE = MergeTree ORDER BY (g, r DESC);
INSERT INTO t_enum_rev VALUES ('manual', 'ok'), ('manual', 'poor'), ('novel', 'great'), ('novel', 'great');
SELECT count() FROM t_enum_rev WHERE g = 'novel' AND r = 'great' SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_enum_rev WHERE g = 'novel' AND r = 'great' SETTINGS use_lightweight_primary_key_index_analysis = 0;
DROP TABLE t_enum_rev;

SELECT 'bounds on the DESC column in each direction';
DROP TABLE IF EXISTS t_str_int;
CREATE TABLE t_str_int (g String, r Int8)
ENGINE = MergeTree ORDER BY (g, r DESC);
INSERT INTO t_str_int VALUES ('manual', 2), ('manual', 1), ('novel', 3), ('novel', 3);
SELECT count() FROM t_str_int WHERE g = 'novel' AND r = 3 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_str_int WHERE g = 'novel' AND r = 3 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_str_int WHERE g = 'novel' AND r >= 3;
SELECT count() FROM t_str_int WHERE g = 'novel' AND r <= 3;
SELECT count() FROM t_str_int WHERE g >= 'novel' AND r = 3;
SELECT count() FROM t_str_int WHERE g = 'manual' AND r > 1;
SELECT count() FROM t_str_int WHERE g = 'manual' AND r = 1;
SELECT count() FROM t_str_int WHERE g = 'novel' AND r = 2;
DROP TABLE t_str_int;

SELECT 'DESC column first';
DROP TABLE IF EXISTS t_rev_first;
CREATE TABLE t_rev_first (r Int8, g String)
ENGINE = MergeTree ORDER BY (r DESC, g);
INSERT INTO t_rev_first VALUES (2, 'manual'), (1, 'manual'), (3, 'novel'), (3, 'zzz');
SELECT count() FROM t_rev_first WHERE r = 3 AND g = 'novel' SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_rev_first WHERE r = 3 AND g = 'novel' SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_rev_first WHERE r = 1 AND g = 'manual';
SELECT count() FROM t_rev_first WHERE r >= 2 AND g >= 'a';
SELECT count() FROM t_rev_first WHERE r = 3;
DROP TABLE t_rev_first;

SELECT 'two DESC columns';
DROP TABLE IF EXISTS t_both_rev;
CREATE TABLE t_both_rev (a UInt8, b UInt8)
ENGINE = MergeTree ORDER BY (a DESC, b DESC) SETTINGS index_granularity = 2;
INSERT INTO t_both_rev SELECT 1 + intDiv(number, 3), 1 + number % 3 FROM numbers(9);
SELECT count() FROM t_both_rev WHERE a = 2 AND b = 2 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_both_rev WHERE a = 2 AND b = 2 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_both_rev WHERE a = 2 AND b >= 2;
SELECT count() FROM t_both_rev WHERE a >= 2 AND b <= 2;
DROP TABLE t_both_rev;

SELECT 'NULLs sit physically first on a DESC column';
DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null (g String, r Nullable(Int8))
ENGINE = MergeTree ORDER BY (g, r DESC)
SETTINGS allow_nullable_key = 1, index_granularity = 2;
INSERT INTO t_null VALUES ('a', NULL), ('a', 5), ('a', 3), ('b', NULL), ('b', NULL), ('b', 7);
SELECT count() FROM t_null WHERE g = 'b' AND r = 7 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_null WHERE g = 'b' AND r = 7 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_null WHERE g = 'a' AND r >= 4;
SELECT count() FROM t_null WHERE g = 'b' AND r IS NULL;
SELECT count() FROM t_null WHERE g = 'b' AND r IS NOT NULL;
SELECT count() FROM t_null WHERE g = 'a' AND r IS NOT NULL;
SELECT count() FROM t_null WHERE r IS NULL;
DROP TABLE t_null;

SELECT 'binary search over many granules';
DROP TABLE IF EXISTS t_big;
CREATE TABLE t_big (g UInt32, r UInt32)
ENGINE = MergeTree ORDER BY (g, r DESC) SETTINGS index_granularity = 4;
INSERT INTO t_big SELECT number % 10, 1000 - number FROM numbers(1000);
SELECT count() FROM t_big WHERE g = 5 AND r = 995 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_big WHERE g = 5 AND r = 995 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_big WHERE g = 5 AND r >= 900;
SELECT count() FROM t_big WHERE g = 5 AND r BETWEEN 500 AND 600;
SELECT count() FROM t_big WHERE g = 9 AND r < 100;
SELECT count() FROM t_big WHERE r = 995;
SELECT count() FROM t_big WHERE g = 5 AND toInt64(r) >= 900;
SELECT count() FROM t_big WHERE g = 5 AND r IN (995, 5, 123);

SELECT 'pruning is still effective, not just correct';
SELECT substring(explain, position(explain, 'Granules:')) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_big WHERE g = 5 AND r = 945 SETTINGS use_lightweight_primary_key_index_analysis = 1) WHERE explain LIKE '%Granules: %/%';
SELECT substring(explain, position(explain, 'Granules:')) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_big WHERE g = 5 AND r = 945 SETTINGS use_lightweight_primary_key_index_analysis = 0) WHERE explain LIKE '%Granules: %/%';
SELECT substring(explain, position(explain, 'Granules:')) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_big WHERE g = 5 AND r >= 900 SETTINGS use_lightweight_primary_key_index_analysis = 1) WHERE explain LIKE '%Granules: %/%';
DROP TABLE t_big;

SELECT 'part without a final mark';
DROP TABLE IF EXISTS t_nofinal;
CREATE TABLE t_nofinal (g UInt8, r UInt8)
ENGINE = MergeTree ORDER BY (g, r DESC)
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_nofinal SELECT 1 + intDiv(number, 5), 5 - number % 5 FROM numbers(10);
SELECT count() FROM t_nofinal WHERE g = 2 AND r = 1 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_nofinal WHERE g = 2 AND r = 1 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_nofinal WHERE g = 2 AND r <= 2;
SELECT count() FROM t_nofinal WHERE g = 2 AND r >= 4;
SELECT count() FROM t_nofinal WHERE g = 1 AND r = 1;
DROP TABLE t_nofinal;

SELECT 'middle DESC column not referenced by the filter';
DROP TABLE IF EXISTS t_skip;
CREATE TABLE t_skip (a UInt16, b UInt16, c UInt16)
ENGINE = MergeTree ORDER BY (a, b DESC, c) SETTINGS index_granularity = 4;
INSERT INTO t_skip SELECT intDiv(number, 100), 9 - intDiv(number % 100, 10), number % 10 FROM numbers(1000);
SELECT count() FROM t_skip WHERE a = 5 AND c = 7 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_skip WHERE a = 5 AND c = 7 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_skip WHERE a = 5 AND b = 3 AND c = 7;
SELECT count() FROM t_skip WHERE a = 5 AND b >= 8 AND c <= 1;
SELECT count() FROM t_skip WHERE a = 5 AND b = 3 AND c >= 8;
DROP TABLE t_skip;

SELECT 'DESC key column not loaded in the in-memory index';
DROP TABLE IF EXISTS t_unloaded;
CREATE TABLE t_unloaded (a UInt16, b UInt16, c UInt16)
ENGINE = MergeTree ORDER BY (a, b DESC, c)
SETTINGS index_granularity = 4,
         primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0;
INSERT INTO t_unloaded SELECT intDiv(number, 100), 9 - intDiv(number % 100, 10), number % 10 FROM numbers(1000);
SELECT count() FROM t_unloaded WHERE a = 5 AND b = 3 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_unloaded WHERE a = 5 AND b = 3 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_unloaded WHERE a = 5 AND b = 3 AND c = 7;
DROP TABLE t_unloaded;

SELECT 'explicit PRIMARY KEY clause inherits ORDER BY directions';
DROP TABLE IF EXISTS t_explicit_pk;
CREATE TABLE t_explicit_pk (g String, r Int8)
ENGINE = MergeTree ORDER BY (g, r DESC) PRIMARY KEY (g, r);
INSERT INTO t_explicit_pk VALUES ('manual', 2), ('manual', 1), ('novel', 3), ('novel', 3);
SELECT count() FROM t_explicit_pk WHERE g = 'novel' AND r = 3 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_explicit_pk WHERE g = 'novel' AND r = 3 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_explicit_pk WHERE g = 'novel' AND r >= 3;
DROP TABLE t_explicit_pk;

SELECT 'ascending control';
DROP TABLE IF EXISTS t_asc;
CREATE TABLE t_asc (g String, r Int8) ENGINE = MergeTree ORDER BY (g, r);
INSERT INTO t_asc VALUES ('manual', 2), ('manual', 1), ('novel', 3), ('novel', 3);
SELECT count() FROM t_asc WHERE g = 'novel' AND r = 3;
DROP TABLE t_asc;
