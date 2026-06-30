-- Tags: no-random-merge-tree-settings

-- Verifies the opt-out / inheritance contract of the default-on
-- `add_minmax_index_for_numeric_columns` setting (https://github.com/ClickHouse/ClickHouse/pull/76867):
--   * the setting is read-only, so an existing table cannot be flipped on with `ALTER`;
--   * an explicit `= 0` opt-out is preserved across an unrelated settings-only `ALTER`;
--   * projection parts never materialize the implicit min-max indices, so a projection
--     cannot smuggle them into a table that opted out.

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

-- With the opt-out, a projection does not add implicit indices anywhere (the projection part
-- carries only primary.cidx / data.bin, no skp_idx_* files).
CREATE TABLE t_minmax_projection_off (a UInt32, b UInt32, PROJECTION p (SELECT b, sum(a) GROUP BY b))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_minmax_projection_off SELECT number, number % 10 FROM numbers(100);

SELECT 'projection opt-out implicit count', count()
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_minmax_projection_off' AND creation = 'Implicit';

-- When enabled, only the main table gets the implicit indices; the projection part still has none.
CREATE TABLE t_minmax_projection_on (a UInt32, b UInt32, PROJECTION p (SELECT b, sum(a) GROUP BY b))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_minmax_projection_on SELECT number, number % 10 FROM numbers(100);

SELECT 'projection enabled main implicit indices', name
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_minmax_projection_on' AND creation = 'Implicit'
ORDER BY name;

DROP TABLE t_minmax_readonly;
DROP TABLE t_minmax_alter_optout;
DROP TABLE t_minmax_projection_off;
DROP TABLE t_minmax_projection_on;
