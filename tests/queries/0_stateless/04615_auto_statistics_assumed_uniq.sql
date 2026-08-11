DROP TABLE IF EXISTS t_auto_stats_float_assume;
DROP TABLE IF EXISTS t_auto_stats_float_normal;
DROP TABLE IF EXISTS t_auto_stats_explicit_float_exact;
DROP TABLE IF EXISTS t_auto_stats_explicit_declared_assume;
DROP TABLE IF EXISTS t_auto_stats_long_string_assume;
DROP TABLE IF EXISTS t_auto_stats_short_string_normal;
DROP TABLE IF EXISTS t_auto_stats_long_string_disabled;
DROP TABLE IF EXISTS t_auto_stats_low_cardinality_float_normal;
DROP TABLE IF EXISTS t_auto_stats_default_heavy_float_normal;
DROP TABLE IF EXISTS t_auto_stats_materialize_assume;
DROP TABLE IF EXISTS t_auto_stats_merge_assume;
DROP TABLE IF EXISTS t_auto_stats_disable_rebuild;

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

-- An explicit STATISTICS(uniq_v2) clause requests a real sketch: the auto_* setting must not degrade it.
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

SELECT 'explicit float stays exact';
SELECT column, has(statistics, 'UniqV2') AS has_uniq_v2, estimates.cardinality < 100 AS normal_cardinality
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

-- A column dominated by repeated default values contradicts the all-distinct assumption
-- (ratio_of_defaults_for_sparse_serialization is the threshold), so the real sketch is kept.
CREATE TABLE t_auto_stats_default_heavy_float_normal
(
    x Float64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic, uniq_v2',
    auto_statistics_assume_floats_distinct = 1,
    ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO t_auto_stats_default_heavy_float_normal
SELECT if(number % 10 = 0, toFloat64(number), 0.) FROM numbers(10000);

SELECT 'default heavy float normal';
SELECT column, has(statistics, 'UniqV2') AS has_uniq_v2, estimates.cardinality < 5000 AS normal_cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_stats_default_heavy_float_normal' AND active AND column = 'x'
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

-- After disabling the assumption setting, merges rebuild the statistics from data
-- (with materialize_statistics_on_merge) instead of propagating the stale assumption.
SET materialize_statistics_on_insert = 1;

CREATE TABLE t_auto_stats_disable_rebuild
(
    x Float64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic, uniq_v2',
    auto_statistics_assume_floats_distinct = 1,
    materialize_statistics_on_merge = 1;

INSERT INTO t_auto_stats_disable_rebuild
SELECT toFloat64(number % 5) FROM numbers(1000);
INSERT INTO t_auto_stats_disable_rebuild
SELECT toFloat64(number % 5) FROM numbers(1000);

SELECT 'before disable';
SELECT column, has(statistics, 'UniqV2') AS has_uniq_v2, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_stats_disable_rebuild' AND active AND column = 'x'
ORDER BY name;

ALTER TABLE t_auto_stats_disable_rebuild MODIFY SETTING auto_statistics_assume_floats_distinct = 0;
OPTIMIZE TABLE t_auto_stats_disable_rebuild FINAL;

SELECT 'after disable and merge';
SELECT column, has(statistics, 'UniqV2') AS has_uniq_v2, estimates.cardinality < 100 AS normal_cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_stats_disable_rebuild' AND active AND column = 'x'
ORDER BY name;

DROP TABLE t_auto_stats_float_assume;
DROP TABLE t_auto_stats_float_normal;
DROP TABLE t_auto_stats_explicit_float_exact;
DROP TABLE t_auto_stats_explicit_declared_assume;
DROP TABLE t_auto_stats_long_string_assume;
DROP TABLE t_auto_stats_short_string_normal;
DROP TABLE t_auto_stats_long_string_disabled;
DROP TABLE t_auto_stats_low_cardinality_float_normal;
DROP TABLE t_auto_stats_default_heavy_float_normal;
DROP TABLE t_auto_stats_materialize_assume;
DROP TABLE t_auto_stats_merge_assume;
DROP TABLE t_auto_stats_disable_rebuild;
