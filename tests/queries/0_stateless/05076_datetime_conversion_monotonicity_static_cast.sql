-- `toDateTime32` of a source type that has no dedicated conversion branch is a plain `static_cast<UInt32>`,
-- which does not preserve order, so it must not be treated as monotonic during index analysis. Otherwise the
-- converted granule range comes out inverted: an `IN` set raises `Invalid binary search result in
-- MergeTreeSetIndex`, and a comparison silently drops matching granules.

SET session_timezone = 'UTC';
-- Without these a bare `count()` is answered from `_minmax_count_projection`, index analysis never runs,
-- and every assertion below is vacuous.
SET optimize_use_implicit_projections = 0;
SET optimize_trivial_count_query = 0;

DROP TABLE IF EXISTS t_enum_key;
DROP TABLE IF EXISTS t_wide_key;
DROP TABLE IF EXISTS t_u32_key;

-- `index_granularity = 1` and `index_granularity_bytes = 0` make one granule per row whatever the runner randomizes.
-- `min_bytes_for_wide_part = 0` agrees with that: non-adaptive granularity stores only Wide parts, and the server
-- logs a warning when the runner randomizes the threshold to a non-zero value.
-- `auto_statistics_types = ''` keeps the primary key the only pruner, so the granule counts below are
-- attributable to the monotonicity decision rather than to column statistics.

-- An `Enum16` member of -1 becomes 4294967295 while 0 stays 0.
CREATE TABLE t_enum_key (x Enum16('neg' = -1, 'zero' = 0)) ENGINE = MergeTree ORDER BY x
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
             auto_statistics_types = '';
INSERT INTO t_enum_key VALUES ('neg'), ('zero');

SELECT 'enum, set atom', count() FROM t_enum_key WHERE toDateTime32(x) IN (toDateTime(0), toDateTime(1));
SELECT 'enum, set atom, no index', countIf(toDateTime32(x) IN (toDateTime(0), toDateTime(1))) FROM t_enum_key;
SELECT 'enum, comparison', count() FROM t_enum_key WHERE toDateTime32(x) = toDateTime(0);
SELECT 'enum, comparison, no index', countIf(toDateTime32(x) = toDateTime(0)) FROM t_enum_key;

-- An `Int256` above the 32-bit range wraps: 4294967296 becomes 0, below 4294967295.
CREATE TABLE t_wide_key (x Int256) ENGINE = MergeTree ORDER BY x
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
             auto_statistics_types = '';
INSERT INTO t_wide_key VALUES (4294967295), (4294967296);

SELECT 'wide, set atom', count() FROM t_wide_key WHERE toDateTime32(x) IN (toDateTime(4294967295), toDateTime(4294967294));
SELECT 'wide, set atom, no index', countIf(toDateTime32(x) IN (toDateTime(4294967295), toDateTime(4294967294))) FROM t_wide_key;
SELECT 'wide, comparison', count() FROM t_wide_key WHERE toDateTime32(x) = toDateTime(4294967295);
SELECT 'wide, comparison, no index', countIf(toDateTime32(x) = toDateTime(4294967295)) FROM t_wide_key;

-- A source type that converts without wrapping stays monotonic, so the primary key must still prune.
CREATE TABLE t_u32_key (x UInt32) ENGINE = MergeTree ORDER BY x
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
             auto_statistics_types = '';
INSERT INTO t_u32_key SELECT 100 + number FROM numbers(4);

SELECT 'uint32, granules pruned', count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_u32_key WHERE toDateTime32(x) IN (toDateTime(0), toDateTime(1)))
WHERE explain ILIKE '%Granules: 0/4%';

DROP TABLE t_enum_key;
DROP TABLE t_wide_key;
DROP TABLE t_u32_key;
