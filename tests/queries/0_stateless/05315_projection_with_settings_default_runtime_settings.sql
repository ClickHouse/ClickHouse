-- `name = DEFAULT` in a projection's `WITH SETTINGS` must reach the settings new projection parts are
-- written with, not only the metadata the projection is built from: the runtime settings of a projection
-- are the table settings with the projection's recorded setting changes applied on top.

DROP TABLE IF EXISTS t_proj_default_runtime;

CREATE TABLE t_proj_default_runtime (a UInt64, c UInt64,
    PROJECTION p_inherit (SELECT a, c ORDER BY c),
    PROJECTION p_reset (SELECT a, c ORDER BY c) WITH SETTINGS (index_granularity = DEFAULT))
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi';

INSERT INTO t_proj_default_runtime SELECT number, number * 2 FROM numbers(200);

-- `p_inherit` takes the table's `index_granularity = 1`, `p_reset` the default of 8192.
SELECT name, marks > 100 FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_default_runtime' AND active
ORDER BY name;

-- The same for parts written after the projection metadata is rebuilt from the stored definition.
DETACH TABLE t_proj_default_runtime;
ATTACH TABLE t_proj_default_runtime;

INSERT INTO t_proj_default_runtime SELECT number, number * 2 FROM numbers(200, 200);
OPTIMIZE TABLE t_proj_default_runtime FINAL;

SELECT 'after reattach and merge';
SELECT name, marks > 100 FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_default_runtime' AND active
ORDER BY name;

DROP TABLE t_proj_default_runtime;
