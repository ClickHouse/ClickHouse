-- A projection whose `GROUP BY` uses positional arguments, which `enable_positional_arguments_for_projections`
-- allows for compatibility with the projections defined by the older versions. Such a projection is
-- calculated with the Analyzer as any other one, so check the whole path: the columns written into
-- the projection part on insert and on merge, and the result the optimizer reads back from it.

SET enable_positional_arguments_for_projections = 1;

DROP TABLE IF EXISTS t_projection_positional;

CREATE TABLE t_projection_positional (a UInt64, b String)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 4, materialize_projections_on_insert = 1;

ALTER TABLE t_projection_positional ADD PROJECTION p (SELECT b, sum(a), count() GROUP BY 1);

INSERT INTO t_projection_positional SELECT number % 7, toString(number % 3) FROM numbers(20);
INSERT INTO t_projection_positional SELECT number % 7, toString(number % 3) FROM numbers(13);

SELECT 'columns of the projection parts written on insert';
SELECT arraySort(groupArray(DISTINCT column)) FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_positional' AND name = 'p' AND active;

OPTIMIZE TABLE t_projection_positional FINAL;

SELECT 'columns of the projection part written on merge';
SELECT arraySort(groupArray(DISTINCT column)) FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_positional' AND name = 'p' AND active;

SELECT 'without the projection';
SELECT b, sum(a), count() FROM t_projection_positional GROUP BY b ORDER BY b SETTINGS optimize_use_projections = 0;

-- The projection optimization does not match an aggregate projection when the aggregation is done in
-- order, and it is not supported with parallel replicas, so pin the settings which the test
-- randomization may flip - the test forces the projection.
SET optimize_aggregation_in_order = 0, parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1;

SELECT 'from the projection';
SELECT b, sum(a), count() FROM t_projection_positional GROUP BY b ORDER BY b SETTINGS force_optimize_projection = 1;

DROP TABLE t_projection_positional;
