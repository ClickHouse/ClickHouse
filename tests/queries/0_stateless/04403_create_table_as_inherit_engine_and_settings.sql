-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/47240
-- `CREATE TABLE dst AS src <storage_clause>` (e.g. SETTINGS or ORDER BY) without an explicit ENGINE
-- must inherit the ENGINE of `src` (it used to be silently replaced with the default engine) and
-- merge the explicitly specified settings on top of `src`'s settings (the latter used to be dropped).

DROP TABLE IF EXISTS t_47240_src;
DROP TABLE IF EXISTS t_47240_settings;
DROP TABLE IF EXISTS t_47240_order_by;
DROP TABLE IF EXISTS t_47240_ttl_src;
DROP TABLE IF EXISTS t_47240_from_ttl;
DROP TABLE IF EXISTS t_47240_mv_src;
DROP VIEW IF EXISTS t_47240_mv;
DROP TABLE IF EXISTS t_47240_from_mv;

CREATE TABLE t_47240_src
(
    a UInt64,
    b String,
    ver UInt64
)
ENGINE = ReplacingMergeTree(ver)
PARTITION BY b
ORDER BY a
SETTINGS index_granularity = 4096, min_bytes_for_wide_part = 123;

-- SETTINGS without ENGINE: the engine (including its arguments) and the non-overridden setting
-- (min_bytes_for_wide_part) are inherited; index_granularity is overridden by the specified value.
CREATE TABLE t_47240_settings AS t_47240_src SETTINGS index_granularity = 8192;
SHOW CREATE TABLE t_47240_settings FORMAT TSVRaw;

-- ORDER BY without ENGINE: the engine and all settings are inherited; only ORDER BY is overridden.
CREATE TABLE t_47240_order_by AS t_47240_src ORDER BY b;
SHOW CREATE TABLE t_47240_order_by FORMAT TSVRaw;

-- A table-level TTL must be inherited as well: it lives in the source storage definition, but the later
-- key-copy path only handled keys (PRIMARY KEY/ORDER BY/...), so `... AS src SETTINGS ...` used to drop it.
CREATE TABLE t_47240_ttl_src
(
    a UInt64,
    d Date
)
ENGINE = MergeTree
ORDER BY a
TTL d + INTERVAL 1 DAY
SETTINGS min_bytes_for_wide_part = 123;

CREATE TABLE t_47240_from_ttl AS t_47240_ttl_src SETTINGS index_granularity = 8192;
SHOW CREATE TABLE t_47240_from_ttl FORMAT TSVRaw;

-- The source can be a materialized view: its engine and keys live in the inner storage definition. Inheriting
-- only the engine used to lose the required ORDER BY, so `CREATE TABLE dst AS mv SETTINGS ...` failed.
CREATE TABLE t_47240_mv_src (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW t_47240_mv
ENGINE = ReplacingMergeTree
PARTITION BY b
ORDER BY a
AS SELECT a, b FROM t_47240_mv_src;

CREATE TABLE t_47240_from_mv AS t_47240_mv SETTINGS index_granularity = 8192;
SHOW CREATE TABLE t_47240_from_mv FORMAT TSVRaw;

DROP TABLE t_47240_from_mv;
DROP VIEW t_47240_mv;
DROP TABLE t_47240_mv_src;
DROP TABLE t_47240_from_ttl;
DROP TABLE t_47240_ttl_src;
DROP TABLE t_47240_order_by;
DROP TABLE t_47240_settings;
DROP TABLE t_47240_src;
