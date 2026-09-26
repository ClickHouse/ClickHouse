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
DROP TABLE IF EXISTS t_47240_join_src;
DROP TABLE IF EXISTS t_47240_from_join;
DROP TEMPORARY TABLE IF EXISTS t_47240_temp_from_join;
DROP TABLE IF EXISTS t_47240_ext_src;
DROP TABLE IF EXISTS t_47240_from_ext;
DROP TABLE IF EXISTS t_47240_from_ext_plain;

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

-- The inherited engine can be a non-default one (Join) that declares engine-specific settings sharing their
-- name with query-level settings (e.g. join_use_nulls). The SETTINGS clause must be split against the inherited
-- engine, otherwise join_use_nulls is misclassified as a query setting, hoisted into the context, and dropped
-- from the persisted table definition.
CREATE TABLE t_47240_join_src (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
CREATE TABLE t_47240_from_join AS t_47240_join_src SETTINGS join_use_nulls = 1;
SHOW CREATE TABLE t_47240_from_join FORMAT TSVRaw;

-- A temporary table never inherits the source engine: `setEngine` always resolves it to
-- default_temporary_table_engine (Memory by default), regardless of the AS source. The SETTINGS clause must
-- therefore be split against that engine, not against the inherited source engine. Otherwise join_use_nulls (a
-- Join setting of the source) would be kept in the storage definition and rejected by Memory, instead of being
-- treated as a query setting.
CREATE TEMPORARY TABLE t_47240_temp_from_join AS t_47240_join_src SETTINGS join_use_nulls = 1;
SHOW CREATE TEMPORARY TABLE t_47240_temp_from_join FORMAT TSVRaw;

-- When restore_replace_external_engines_to_null is set, an external engine inherited from the source (here URL)
-- through `CREATE TABLE dst AS src <storage_clause>` must be replaced with Null, just like an explicitly
-- specified external engine is. The source table is created before the setting is enabled so that it keeps its
-- external engine.
CREATE TABLE t_47240_ext_src (x UInt64) ENGINE = URL('http://localhost:1/', 'TSV');
SET restore_replace_external_engines_to_null = 1;
CREATE TABLE t_47240_from_ext AS t_47240_ext_src ORDER BY x;
SHOW CREATE TABLE t_47240_from_ext FORMAT TSVRaw;

-- The same replacement must happen for a plain `CREATE TABLE dst AS src` without any storage clause: the
-- engine is still inherited from the source, so the inherited external URL engine must become Null too, rather
-- than only the partial-storage-clause form above being replaced.
CREATE TABLE t_47240_from_ext_plain AS t_47240_ext_src;
SHOW CREATE TABLE t_47240_from_ext_plain FORMAT TSVRaw;
SET restore_replace_external_engines_to_null = 0;

DROP TABLE t_47240_from_ext_plain;
DROP TABLE t_47240_from_ext;
DROP TABLE t_47240_ext_src;
DROP TEMPORARY TABLE t_47240_temp_from_join;
DROP TABLE t_47240_from_join;
DROP TABLE t_47240_join_src;
DROP TABLE t_47240_from_mv;
DROP VIEW t_47240_mv;
DROP TABLE t_47240_mv_src;
DROP TABLE t_47240_from_ttl;
DROP TABLE t_47240_ttl_src;
DROP TABLE t_47240_order_by;
DROP TABLE t_47240_settings;
DROP TABLE t_47240_src;
