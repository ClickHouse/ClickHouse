-- https://github.com/ClickHouse/ClickHouse/issues/111231
-- Column include/exclude lists for automatic statistics.

DROP TABLE IF EXISTS t_auto_stats_exclude;
DROP TABLE IF EXISTS t_auto_stats_include;
DROP TABLE IF EXISTS t_auto_stats_both;
DROP TABLE IF EXISTS t_auto_stats_explicit;
DROP TABLE IF EXISTS t_auto_stats_alter;

SELECT 'exclude';
CREATE TABLE t_auto_stats_exclude
(
    a UInt64,
    b UInt64,
    c String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    auto_statistics_types = 'basic, uniq',
    auto_statistics_exclude_columns = 'b, c';

SELECT name, statistics != '' AS has_stats
FROM system.columns
WHERE database = currentDatabase() AND table = 't_auto_stats_exclude'
ORDER BY name;

SELECT 'include';
CREATE TABLE t_auto_stats_include
(
    a UInt64,
    b UInt64,
    c String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    auto_statistics_types = 'basic, uniq',
    auto_statistics_columns = 'a, c';

SELECT name, statistics != '' AS has_stats
FROM system.columns
WHERE database = currentDatabase() AND table = 't_auto_stats_include'
ORDER BY name;

SELECT 'include_and_exclude';
CREATE TABLE t_auto_stats_both
(
    a UInt64,
    b UInt64,
    c String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    auto_statistics_types = 'basic, uniq',
    auto_statistics_columns = 'a, b, c',
    auto_statistics_exclude_columns = 'c';

SELECT name, statistics != '' AS has_stats
FROM system.columns
WHERE database = currentDatabase() AND table = 't_auto_stats_both'
ORDER BY name;

SELECT 'explicit_keeps_despite_exclude';
CREATE TABLE t_auto_stats_explicit
(
    a UInt64,
    b UInt64 STATISTICS(basic),
    c String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    auto_statistics_types = 'basic, uniq',
    auto_statistics_exclude_columns = 'b, c';

SELECT name, statistics != '' AS has_stats
FROM system.columns
WHERE database = currentDatabase() AND table = 't_auto_stats_explicit'
ORDER BY name;

SELECT 'alter_exclude';
CREATE TABLE t_auto_stats_alter
(
    a UInt64,
    b UInt64,
    c String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS auto_statistics_types = 'basic, uniq';

SELECT name, statistics != '' AS has_stats
FROM system.columns
WHERE database = currentDatabase() AND table = 't_auto_stats_alter'
ORDER BY name;

ALTER TABLE t_auto_stats_alter MODIFY SETTING auto_statistics_exclude_columns = 'b, c';

SELECT name, statistics != '' AS has_stats
FROM system.columns
WHERE database = currentDatabase() AND table = 't_auto_stats_alter'
ORDER BY name;

SELECT 'unknown_column_names';
-- Unknown column names in the lists must not fail CREATE / ATTACH paths.
CREATE TABLE t_auto_stats_unknown
(
    a UInt64
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    auto_statistics_types = 'basic',
    auto_statistics_columns = 'a, missing_col',
    auto_statistics_exclude_columns = 'also_missing';

SELECT name, statistics != '' AS has_stats
FROM system.columns
WHERE database = currentDatabase() AND table = 't_auto_stats_unknown'
ORDER BY name;

DROP TABLE t_auto_stats_exclude;
DROP TABLE t_auto_stats_include;
DROP TABLE t_auto_stats_both;
DROP TABLE t_auto_stats_explicit;
DROP TABLE t_auto_stats_alter;
DROP TABLE t_auto_stats_unknown;
