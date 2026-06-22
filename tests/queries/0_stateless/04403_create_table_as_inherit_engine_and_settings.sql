-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/47240
-- `CREATE TABLE dst AS src <storage_clause>` (e.g. SETTINGS or ORDER BY) without an explicit ENGINE
-- must inherit the ENGINE of `src` (it used to be silently replaced with the default engine) and
-- merge the explicitly specified settings on top of `src`'s settings (the latter used to be dropped).

DROP TABLE IF EXISTS t_47240_src;
DROP TABLE IF EXISTS t_47240_settings;
DROP TABLE IF EXISTS t_47240_order_by;

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

DROP TABLE t_47240_order_by;
DROP TABLE t_47240_settings;
DROP TABLE t_47240_src;
