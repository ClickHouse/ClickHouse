-- Tags: no-random-merge-tree-settings

-- Verifies the opt-out / inheritance contract of the default-on
-- `add_minmax_index_for_numeric_columns` setting (https://github.com/ClickHouse/ClickHouse/pull/76867):
--   * the setting is read-only, so an existing table cannot be flipped on with `ALTER`;
--   * an explicit `= 0` opt-out is preserved across an unrelated settings-only `ALTER`;
--   * the main table's implicit indices follow the table-level setting.
--
-- Note: `system.data_skipping_indices` lists only the main table's top-level indices, not the
-- indices declared on projection parts. A projection does NOT inherit the parent table's
-- `add_minmax_index_for_numeric_columns`: a projection without an explicit `WITH SETTINGS` is
-- seeded from the global default in `ProjectionDescription::getProjectionFromAST`, so the
-- per-projection `WITH SETTINGS` clause (not the parent setting) is what opts a projection in or
-- out. That behavior is covered separately by `04242_projection_implicit_minmax_via_settings`.

DROP TABLE IF EXISTS t_minmax_readonly;
DROP TABLE IF EXISTS t_minmax_alter_optout;
DROP TABLE IF EXISTS t_minmax_projection_off;
DROP TABLE IF EXISTS t_minmax_projection_on;

-- `add_minmax_index_for_numeric_columns` is a read-only MergeTree setting: it cannot be
-- enabled on an existing table, so a `= 0` table can never be flipped to implicit indices.
CREATE TABLE t_minmax_readonly (a UInt32) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns = 0;
ALTER TABLE t_minmax_readonly MODIFY SETTING add_minmax_index_for_numeric_columns = 1; -- { serverError READONLY_SETTING }

SELECT 'readonly opt-out preserved', count()
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_minmax_readonly' AND creation = 'Implicit';

-- An explicit opt-out must survive an unrelated settings-only `ALTER` (no stale-metadata flip:
-- implicit indices are baked into the schema at creation, not re-derived from the setting).
CREATE TABLE t_minmax_alter_optout (a UInt32, b Int64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns = 0;
ALTER TABLE t_minmax_alter_optout MODIFY SETTING merge_max_block_size = 8192;

SELECT 'unrelated alter keeps opt-out', count()
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_minmax_alter_optout' AND creation = 'Implicit';

-- With the opt-out on the main table, the main table carries no implicit indices; the presence
-- of a projection does not change the main-table contract.
CREATE TABLE t_minmax_projection_off (a UInt32, b UInt32, PROJECTION p (SELECT b, sum(a) GROUP BY b))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_minmax_projection_off SELECT number, number % 10 FROM numbers(100);

SELECT 'parent opt-out: main-table implicit count', count()
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_minmax_projection_off' AND creation = 'Implicit';

-- When enabled, the main table's numeric columns get the implicit indices.
CREATE TABLE t_minmax_projection_on (a UInt32, b UInt32, PROJECTION p (SELECT b, sum(a) GROUP BY b))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_minmax_projection_on SELECT number, number % 10 FROM numbers(100);

SELECT 'parent enabled: main-table implicit indices', name
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_minmax_projection_on' AND creation = 'Implicit'
ORDER BY name;

DROP TABLE t_minmax_readonly;
DROP TABLE t_minmax_alter_optout;
DROP TABLE t_minmax_projection_off;
DROP TABLE t_minmax_projection_on;
