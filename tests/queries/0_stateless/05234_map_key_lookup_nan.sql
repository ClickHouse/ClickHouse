-- A `NaN` key of a Map is found by a requested `NaN` and by nothing else, and a requested `NaN`
-- finds nothing but a `NaN` key. The subcolumn extractor used to ask `compareAt` with a direction
-- hint of zero, which answers with the hint itself when one side is a `NaN`, so a row holding a
-- `NaN` key matched every requested key and a requested `NaN` matched every key.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_map_nan_key;

-- The specialized matchers: BFloat16, Float32 and Float64 keys are compared value by value.

CREATE TABLE t_map_nan_key (id UInt64, m Map(Float64, String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_nan_key VALUES (1, map(nan, 'nan-hit', 1.5, 'one-five')), (2, map(2.5, 'other')), (3, map(1.5, 'plain'));
SELECT 'Float64', id, `m.key_nan`, `m.key_1.5`, `m.key_2.5` FROM t_map_nan_key ORDER BY id;
DROP TABLE t_map_nan_key;

CREATE TABLE t_map_nan_key (id UInt64, m Map(Float32, String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_nan_key VALUES (1, map(nan, 'nan-hit', 1.5, 'one-five')), (2, map(2.5, 'other')), (3, map(1.5, 'plain'));
SELECT 'Float32', id, `m.key_nan`, `m.key_1.5`, `m.key_2.5` FROM t_map_nan_key ORDER BY id;
DROP TABLE t_map_nan_key;

CREATE TABLE t_map_nan_key (id UInt64, m Map(BFloat16, String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_nan_key VALUES (1, map(nan, 'nan-hit', 1.5, 'one-five')), (2, map(2.5, 'other')), (3, map(1.5, 'plain'));
SELECT 'BFloat16', id, `m.key_nan`, `m.key_1.5`, `m.key_2.5` FROM t_map_nan_key ORDER BY id;
DROP TABLE t_map_nan_key;

-- The generic matcher: a LowCardinality dictionary that does not hold strings, and a Tuple, are
-- compared through the virtual `compareAt`.

CREATE TABLE t_map_nan_key (id UInt64, m Map(LowCardinality(Float64), String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_nan_key VALUES (1, map(nan, 'nan-hit', 1.5, 'one-five')), (2, map(2.5, 'other')), (3, map(1.5, 'plain'));
SELECT 'LowCardinality(Float64)', id, `m.key_nan`, `m.key_1.5`, `m.key_2.5` FROM t_map_nan_key ORDER BY id;
DROP TABLE t_map_nan_key;

CREATE TABLE t_map_nan_key (id UInt64, m Map(Tuple(Float64, UInt8), String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_nan_key VALUES (1, map((nan, 1), 'nan-hit', (1.5, 1), 'one-five')), (2, map((2.5, 1), 'other')), (3, map((1.5, 1), 'plain'));
SELECT 'Tuple(Float64, UInt8)', id, `m.key_(nan,1)`, `m.key_(1.5,1)`, `m.key_(2.5,1)` FROM t_map_nan_key ORDER BY id;
DROP TABLE t_map_nan_key;

-- The same hint decides where a NULL goes, so a NULL inside a composite key used to be equal to
-- every other value as well.

CREATE TABLE t_map_nan_key (id UInt64, m Map(Tuple(Nullable(UInt8), UInt8), String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_nan_key VALUES (1, map((NULL, 1), 'null-hit', (5, 1), 'five')), (2, map((7, 1), 'other')), (3, map((5, 1), 'plain'));
SELECT 'Tuple(Nullable(UInt8), UInt8)', id, `m.key_(NULL,1)`, `m.key_(5,1)`, `m.key_(7,1)` FROM t_map_nan_key ORDER BY id;
DROP TABLE t_map_nan_key;

-- A `NaN` in the value is copied as it is, whichever key it is reached by.

CREATE TABLE t_map_nan_key (id UInt64, m Map(String, Float64))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_nan_key VALUES (1, map('a', nan, 'b', 1.5)), (2, map('b', 2.5));
SELECT 'NaN value', id, isNaN(m['a']), m['b'] FROM t_map_nan_key ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'NaN value', id, isNaN(m['a']), m['b'] FROM t_map_nan_key ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_nan_key;
