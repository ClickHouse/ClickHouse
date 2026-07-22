DROP TABLE IF EXISTS t_auto_stats_float_assume;
DROP TABLE IF EXISTS t_auto_stats_float_normal;
DROP TABLE IF EXISTS t_auto_stats_explicit_float_exact;
DROP TABLE IF EXISTS t_auto_stats_explicit_declared_assume;
DROP TABLE IF EXISTS t_auto_stats_long_string_assume;
DROP TABLE IF EXISTS t_auto_stats_short_string_normal;
DROP TABLE IF EXISTS t_auto_stats_long_string_disabled;
DROP TABLE IF EXISTS t_auto_stats_low_cardinality_float_normal;
DROP TABLE IF EXISTS t_auto_stats_materialize_assume;
DROP TABLE IF EXISTS t_auto_stats_merge_assume;

SET allow_statistics = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET materialize_statistics_on_insert = 1;

CREATE TABLE t_auto_stats_float_assume
(
    x Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic, uniq_v2',
    auto_statistics_assume_floats_distinct = 1;

INSERT INTO t_auto_stats_float_assume
SELECT if(number % 10 = 0, NULL, toFloat64(number % 5)) FROM numbers(1000);

SELECT 'float assume';
SELECT column, has(statistics, 'UniqV2') AS has_uniq_v2, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_stats_float_assume' AND active AND column = 'x'
ORDER BY name;

CREATE TABLE t_auto_stats_float_normal
(
    x Float64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic, uniq_v2',
    auto_statistics_assume_floats_distinct = 0;

INSERT INTO t_auto_stats_float_normal
SELECT toFloat64(number % 5) FROM numbers(1000);

SELECT 'float disabled';
SELECT column, has(statistics, 'UniqV2') AS has_uniq_v2, estimates.cardinality < 100 AS normal_cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_stats_float_normal' AND active AND column = 'x'
ORDER BY name;

CREATE TABLE t_auto_stats_explicit_float_exact
(
    x Float64 STATISTICS(uniq_v2)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = '',
    auto_statistics_assume_floats_distinct = 1;

INSERT INTO t_auto_stats_explicit_float_exact
SELECT toFloat64(number % 5) FROM numbers(1000);

SELECT 'explicit float assume';
SELECT column, has(statistics, 'UniqV2') AS has_uniq_v2, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_stats_explicit_float_exact' AND active AND column = 'x'
ORDER BY name;

CREATE TABLE t_auto_stats_explicit_declared_assume
(
    x Float64 STATISTICS(uniq_v2(assumed_all_distinct))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = '',
    auto_statistics_assume_floats_distinct = 0;

INSERT INTO t_auto_stats_explicit_declared_assume
SELECT toFloat64(number % 5) FROM numbers(1000);

SELECT 'explicit declared assume';
SELECT column, has(statistics, 'UniqV2') AS has_uniq_v2, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_stats_explicit_declared_assume' AND active AND column = 'x'
ORDER BY name;

CREATE TABLE t_auto_stats_long_string_assume
(
    s Nullable(String)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic, uniq_v2',
    auto_statistics_assume_long_strings_distinct = 1,
    auto_statistics_long_string_distinct_min_length = 64,
    auto_statistics_long_string_distinct_probe_rows = 1000;

INSERT INTO t_auto_stats_long_string_assume
SELECT if(number % 10 = 0, NULL, repeat('x', 80)) FROM numbers(1000);

SELECT 'long string assume';
SELECT column, has(statistics, 'UniqV2') AS has_uniq_v2, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_stats_long_string_assume' AND active AND column = 's'
ORDER BY name;

CREATE TABLE t_auto_stats_short_string_normal
(
    s String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic, uniq_v2',
    auto_statistics_assume_long_strings_distinct = 1,
    auto_statistics_long_string_distinct_min_length = 64,
    auto_statistics_long_string_distinct_probe_rows = 1000;

INSERT INTO t_auto_stats_short_string_normal
SELECT toString(number % 5) FROM numbers(1000);

SELECT 'short string normal';
SELECT column, has(statistics, 'UniqV2') AS has_uniq_v2, estimates.cardinality < 100 AS normal_cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_stats_short_string_normal' AND active AND column = 's'
ORDER BY name;

CREATE TABLE t_auto_stats_long_string_disabled
(
    s String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic, uniq_v2',
    auto_statistics_assume_long_strings_distinct = 0,
    auto_statistics_long_string_distinct_min_length = 64,
    auto_statistics_long_string_distinct_probe_rows = 1000;

INSERT INTO t_auto_stats_long_string_disabled
SELECT repeat('x', 80) FROM numbers(1000);

SELECT 'long string disabled';
SELECT column, has(statistics, 'UniqV2') AS has_uniq_v2, estimates.cardinality < 100 AS normal_cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_stats_long_string_disabled' AND active AND column = 's'
ORDER BY name;

CREATE TABLE t_auto_stats_low_cardinality_float_normal
(
    x LowCardinality(Float64)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic, uniq_v2',
    auto_statistics_assume_floats_distinct = 1,
    materialize_statistics_on_merge = 1;

INSERT INTO t_auto_stats_low_cardinality_float_normal
SELECT toFloat64(number % 5) FROM numbers(1000);
INSERT INTO t_auto_stats_low_cardinality_float_normal
SELECT toFloat64(number % 5) FROM numbers(1000);
OPTIMIZE TABLE t_auto_stats_low_cardinality_float_normal FINAL;

SELECT 'low cardinality float normal';
SELECT column, has(statistics, 'UniqV2') AS has_uniq_v2, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_stats_low_cardinality_float_normal' AND active AND column = 'x'
ORDER BY name;

SET materialize_statistics_on_insert = 0;

CREATE TABLE t_auto_stats_materialize_assume
(
    x Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic, uniq_v2',
    auto_statistics_assume_floats_distinct = 1;

INSERT INTO t_auto_stats_materialize_assume
SELECT if(number % 10 = 0, NULL, toFloat64(number % 5)) FROM numbers(1000);
ALTER TABLE t_auto_stats_materialize_assume MATERIALIZE STATISTICS x SETTINGS mutations_sync = 1;

SELECT 'materialize assume';
SELECT column, has(statistics, 'UniqV2') AS has_uniq_v2, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_stats_materialize_assume' AND active AND column = 'x'
ORDER BY name;

CREATE TABLE t_auto_stats_merge_assume
(
    x Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic, uniq_v2',
    auto_statistics_assume_floats_distinct = 1,
    materialize_statistics_on_merge = 1;

INSERT INTO t_auto_stats_merge_assume
SELECT if(number % 10 = 0, NULL, toFloat64(number % 5)) FROM numbers(1000);
INSERT INTO t_auto_stats_merge_assume
SELECT if(number % 10 = 0, NULL, toFloat64(number % 5)) FROM numbers(1000);
OPTIMIZE TABLE t_auto_stats_merge_assume FINAL;

SELECT 'merge assume';
SELECT column, has(statistics, 'UniqV2') AS has_uniq_v2, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_stats_merge_assume' AND active AND column = 'x'
ORDER BY name;

DROP TABLE t_auto_stats_float_assume;
DROP TABLE t_auto_stats_float_normal;
DROP TABLE t_auto_stats_explicit_float_exact;
DROP TABLE t_auto_stats_explicit_declared_assume;
DROP TABLE t_auto_stats_long_string_assume;
DROP TABLE t_auto_stats_short_string_normal;
DROP TABLE t_auto_stats_long_string_disabled;
DROP TABLE t_auto_stats_low_cardinality_float_normal;
DROP TABLE t_auto_stats_materialize_assume;
DROP TABLE t_auto_stats_merge_assume;
