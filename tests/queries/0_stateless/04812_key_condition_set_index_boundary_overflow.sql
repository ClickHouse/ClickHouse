-- { echo }
-- Set-index range boundaries must be compared at their true value. An excluded boundary at a key
-- column type's maximum is shrunk to one above the domain (255 -> 256 for UInt8); storing it used to
-- truncate it back into the domain, where it could land on a set element and report a match for a
-- range holding none. Debug builds then aborted with `Inconsistent KeyCondition behavior`.
-- The mirror case is a boundary at a signed type's minimum, shrunk one below the domain.

SET enable_analyzer = 1;
SET use_lightweight_primary_key_index_analysis = 1;
-- The query-condition cache prunes granules before primary-key analysis, and the predicate below
-- runs more than once, so a warm entry would satisfy the row cap without the set index being right.
SET use_query_condition_cache = 0;
-- Prevent remote replicas from skipping index analysis in Parallel Replicas. Otherwise, they may return full ranges and trigger max_rows_to_read validation failures.
SET parallel_replicas_index_analysis_only_on_coordinator = 0;
-- Without the implicit count() projection the exact-range path is never requested, so every witness
-- below passes whether or not the set index is right.
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;

-- Witness: UInt8 key at the type maximum, 1-element set whose element is absent.
DROP TABLE IF EXISTS t_setidx_u8;
CREATE TABLE t_setidx_u8 (a UInt64, b UInt8, c String) ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 1;
INSERT INTO t_setidx_u8 SELECT number % 5, 255, toString(number) FROM numbers(20);
SELECT count() FROM t_setidx_u8 WHERE a = 1 AND b IN (0);
-- Control: one below the maximum cannot be pushed out of the domain.
DROP TABLE IF EXISTS t_setidx_u8_254;
CREATE TABLE t_setidx_u8_254 (a UInt64, b UInt8, c String) ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 1;
INSERT INTO t_setidx_u8_254 SELECT number % 5, 254, toString(number) FROM numbers(20);
SELECT count() FROM t_setidx_u8_254 WHERE a = 1 AND b IN (0);
-- Control: the set element is present, so the answer is non-zero and must stay non-zero.
SELECT count() FROM t_setidx_u8 WHERE a = 1 AND b IN (255);
-- Control: NOT IN over the same boundary reads more granules, never fewer.
SELECT count() FROM t_setidx_u8 WHERE a = 1 AND b NOT IN (0);
-- Control: a 2-element set is already relaxed.
SELECT count() FROM t_setidx_u8 WHERE a = 1 AND b IN (0, 1);
-- Control: the set index stays exact for a single-column key, so exact-count still applies.
SELECT count() FROM t_setidx_u8 WHERE a IN (1);
-- Control: an equivalent range atom, which never round-tripped a bound through a typed column.
SELECT count() FROM t_setidx_u8 WHERE a = 1 AND b = 0;

-- Witness: the same defect is type-relative, at UInt16's own maximum.
DROP TABLE IF EXISTS t_setidx_u16;
CREATE TABLE t_setidx_u16 (a UInt64, b UInt16, c String) ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 1;
INSERT INTO t_setidx_u16 SELECT number % 5, 65535, toString(number) FROM numbers(20);
SELECT count() FROM t_setidx_u16 WHERE a = 1 AND b IN (0);

-- Witness: signed key at its maximum.
DROP TABLE IF EXISTS t_setidx_i8;
CREATE TABLE t_setidx_i8 (a UInt64, b Int8, c String) ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 1;
INSERT INTO t_setidx_i8 SELECT number % 5, 127, toString(number) FROM numbers(20);
SELECT count() FROM t_setidx_i8 WHERE a = 1 AND b IN (-128);

-- Witness: the right-bound mirror. An excluded right bound is shrunk by decrementing, so a signed
-- key at its minimum yields a bound one below the domain. Unsigned types are immune here: their
-- minimum is 0, which equals the guard value in Range::shrinkToIncludedIfPossible.
DROP TABLE IF EXISTS t_setidx_i8_min;
CREATE TABLE t_setidx_i8_min (a UInt64, b Int8, c String) ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 1;
INSERT INTO t_setidx_i8_min SELECT number % 5, -128, toString(number) FROM numbers(20);
SELECT count() FROM t_setidx_i8_min WHERE a = 1 AND b IN (127);
DROP TABLE IF EXISTS t_setidx_i16_min;
CREATE TABLE t_setidx_i16_min (a UInt64, b Int16, c String) ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 1;
INSERT INTO t_setidx_i16_min SELECT number % 5, -32768, toString(number) FROM numbers(20);
SELECT count() FROM t_setidx_i16_min WHERE a = 1 AND b IN (32767);

-- Witness: integer-backed Date / DateTime / Enum key columns.
DROP TABLE IF EXISTS t_setidx_date;
CREATE TABLE t_setidx_date (a UInt64, b Date, c String) ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 1;
INSERT INTO t_setidx_date SELECT number % 5, toDate(65535), toString(number) FROM numbers(20);
SELECT count() FROM t_setidx_date WHERE a = 1 AND b IN (toDate(0));
DROP TABLE IF EXISTS t_setidx_dt;
CREATE TABLE t_setidx_dt (a UInt64, b DateTime, c String) ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 1;
INSERT INTO t_setidx_dt SELECT number % 5, toDateTime(4294967295), toString(number) FROM numbers(20);
SELECT count() FROM t_setidx_dt WHERE a = 1 AND b IN (toDateTime(0));
DROP TABLE IF EXISTS t_setidx_enum;
CREATE TABLE t_setidx_enum (a UInt64, b Enum8('lo' = -128, 'hi' = 127), c String)
ENGINE = MergeTree ORDER BY (a, b, c) SETTINGS index_granularity = 1;
INSERT INTO t_setidx_enum SELECT number % 5, 'hi', toString(number) FROM numbers(20);
SELECT count() FROM t_setidx_enum WHERE a = 1 AND b IN ('lo');

-- Witness: has() builds the same set index.
SELECT count() FROM t_setidx_u8 WHERE a = 1 AND has([0], b);

-- Witness: tuple sets, both a full-key tuple and a prefix tuple.
SELECT count() FROM t_setidx_u8 WHERE (a, b) IN ((1, 0));
SELECT count() FROM t_setidx_u8 WHERE (a, b, c) IN ((1, 0, '1'));

-- Witness: a Nullable key column stores its values in the nested column, whose type bounds the domain.
DROP TABLE IF EXISTS t_setidx_nullable;
CREATE TABLE t_setidx_nullable (a UInt64, b Nullable(UInt8), c String) ENGINE = MergeTree
ORDER BY (a, b, c) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_setidx_nullable SELECT number % 5, 255, toString(number) FROM numbers(20);
SELECT count() FROM t_setidx_nullable WHERE a = 1 AND b IN (0);

-- Witness: LowCardinality key wrapper.
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS t_setidx_lc;
CREATE TABLE t_setidx_lc (a UInt64, b LowCardinality(UInt8), c String) ENGINE = MergeTree
ORDER BY (a, b, c) SETTINGS index_granularity = 1;
INSERT INTO t_setidx_lc SELECT number % 5, 255, toString(number) FROM numbers(20);
SELECT count() FROM t_setidx_lc WHERE a = 1 AND b IN (0);

-- Witness: the dense checkInRange overload builds its ranges differently from the sparse one.
SET use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_setidx_u8 WHERE a = 1 AND b IN (0);
SELECT count() FROM t_setidx_i8_min WHERE a = 1 AND b IN (127);
SELECT count() FROM t_setidx_u8 WHERE a = 1 AND b IN (255);
SET use_lightweight_primary_key_index_analysis = 1;

-- Non-integer keys cannot be pushed out of domain: Range::shrinkToIncludedIfPossible only adjusts
-- Int64/UInt64 Fields. Controls, asserting the fix did not change them.
DROP TABLE IF EXISTS t_setidx_f32;
CREATE TABLE t_setidx_f32 (a UInt64, b Float32, c String) ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 1;
INSERT INTO t_setidx_f32 SELECT number % 5, 3.4e38, toString(number) FROM numbers(20);
SELECT count() FROM t_setidx_f32 WHERE a = 1 AND b IN (0);
SELECT count() FROM t_setidx_f32 WHERE a = 1 AND b IN (toFloat32(3.4e38));

-- The set atom must select no more granules than the equivalent range atom. A too-wide mark range
-- reads more rows, so a row cap the range atom satisfies is the observable difference: before the fix
-- the set atom read 5 granules here and exceeded a cap of 4.
SELECT * FROM t_setidx_u8 WHERE a = 1 AND b IN (0) SETTINGS max_rows_to_read = 4;
SELECT * FROM t_setidx_u8 WHERE a = 1 AND b = 0 SETTINGS max_rows_to_read = 4;
-- The same observable difference for the right-bound mirror, whose count() witnesses are correct
-- either way and so cannot redden where the exactness check is compiled out.
SELECT * FROM t_setidx_i8_min WHERE a = 1 AND b IN (127) SETTINGS max_rows_to_read = 4;
SELECT * FROM t_setidx_i8_min WHERE a = 1 AND b = 127 SETTINGS max_rows_to_read = 4;

DROP TABLE t_setidx_u8;
DROP TABLE t_setidx_u8_254;
DROP TABLE t_setidx_u16;
DROP TABLE t_setidx_i8;
DROP TABLE t_setidx_i8_min;
DROP TABLE t_setidx_i16_min;
DROP TABLE t_setidx_date;
DROP TABLE t_setidx_dt;
DROP TABLE t_setidx_enum;
DROP TABLE t_setidx_nullable;
DROP TABLE t_setidx_lc;
DROP TABLE t_setidx_f32;
