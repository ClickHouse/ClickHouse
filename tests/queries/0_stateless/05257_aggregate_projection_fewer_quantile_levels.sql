-- Reading one of several quantile levels stored by an aggregate projection, through a distributed
-- query over a table where only some of the parts carry that projection.

DROP TABLE IF EXISTS t_agg_proj_quantile_levels;

CREATE TABLE t_agg_proj_quantile_levels
(
    a String,
    q AggregateFunction(quantilesTiming(0.5, 0.95, 0.99), Int64)
)
ENGINE = MergeTree ORDER BY a;

SYSTEM STOP MERGES t_agg_proj_quantile_levels;

-- Inserted before the projection is added, so this part has no projection part.
INSERT INTO t_agg_proj_quantile_levels
    SELECT 'x', quantilesTimingState(0.5, 0.95, 0.99)(number::Int64) FROM numbers(1000);

ALTER TABLE t_agg_proj_quantile_levels
    ADD PROJECTION p (SELECT a, quantilesTimingMerge(0.5, 0.95, 0.99)(q) GROUP BY a);

-- Inserted after, so this part does carry the projection. It is large enough that reading the
-- projection stays cheaper than reading the table, so the projection is still worth using.
INSERT INTO t_agg_proj_quantile_levels
    SELECT 'x', quantilesTimingState(0.5, 0.95, 0.99)(number::Int64)
    FROM numbers(20000) GROUP BY number % 20000;

-- Some parts carry the projection and some do not, which is what this test needs.
WITH
    (SELECT count() FROM system.parts
      WHERE database = currentDatabase() AND table = 't_agg_proj_quantile_levels' AND active) AS parts,
    (SELECT count() FROM system.projection_parts
      WHERE database = currentDatabase() AND table = 't_agg_proj_quantile_levels' AND active) AS projection_parts
SELECT parts > projection_parts AND projection_parts > 0;

-- force_optimize_projection_name makes the query fail if projection p is not applied.
SELECT a, quantilesTimingMerge(0.95)(q)
FROM remote('127.0.0.{1,2}', currentDatabase(), t_agg_proj_quantile_levels)
GROUP BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'p';

-- Reading through the projection and reading the table agree.
SELECT
    (SELECT quantilesTimingMerge(0.95)(q) FROM remote('127.0.0.{1,2}', currentDatabase(), t_agg_proj_quantile_levels)
       SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'p')
  = (SELECT quantilesTimingMerge(0.95)(q) FROM remote('127.0.0.{1,2}', currentDatabase(), t_agg_proj_quantile_levels)
       SETTINGS optimize_use_projections = 0);

DROP TABLE t_agg_proj_quantile_levels;
