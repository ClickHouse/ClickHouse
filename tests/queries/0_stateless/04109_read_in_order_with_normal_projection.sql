SET enable_analyzer = 1;
SET optimize_read_in_order = 1;
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
SET optimize_use_projection_filtering = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET read_in_order_use_virtual_row = 1;
SET enable_parallel_replicas = 0;
SET parallel_replicas_for_non_replicated_merge_tree = 0;

DROP TABLE IF EXISTS mt SYNC;

CREATE TABLE mt
(
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 1, auto_statistics_types = 'minmax, uniq';

INSERT INTO mt SELECT 1, 1;
INSERT INTO mt SELECT 2, 2;
INSERT INTO mt SELECT 3, 3;
OPTIMIZE TABLE mt;

ALTER TABLE mt ADD PROJECTION proj (SELECT * ORDER BY b) SETTINGS alter_sync = 2;

INSERT INTO mt SELECT 4, 4;
INSERT INTO mt SELECT 5, 5;
INSERT INTO mt SELECT 6, 6;
OPTIMIZE TABLE mt;

SELECT name FROM system.parts WHERE table = 'mt' AND database = currentDatabase() AND active ORDER BY name;

SELECT '---';

SELECT name, parent_name FROM system.projection_parts WHERE table = 'mt' AND database = currentDatabase() AND active ORDER BY name;

SELECT '---';

EXPLAIN SELECT * FROM mt WHERE b < 5 ORDER BY b;

SELECT '---';

EXPLAIN SELECT * FROM mt WHERE b >= 3 ORDER BY b;

SELECT '---';

DROP TABLE mt SYNC;
