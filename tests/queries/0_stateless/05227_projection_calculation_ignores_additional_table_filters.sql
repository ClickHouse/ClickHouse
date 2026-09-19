-- A projection is calculated over the data being written. The source of the calculation borrows the
-- name of `system.one`, so an `additional_table_filters` entry for that name must not reshape the data
-- of the projection part. Otherwise wrong aggregates are persisted in the projection, and they stay
-- wrong after the setting is gone, because a merge combines the stored states instead of recomputing
-- them from the source data.

DROP TABLE IF EXISTS t_projection_filters;
CREATE TABLE t_projection_filters (a UInt64, b String, PROJECTION p (SELECT b, sum(a), count() GROUP BY b))
ENGINE = MergeTree ORDER BY a SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_filters SELECT number % 3, toString(number % 2) FROM numbers(10)
SETTINGS additional_table_filters = {'system.one' : 'a != 0'};

-- The projection must give the same answer as the table itself.
SELECT b, sum(a), count() FROM t_projection_filters GROUP BY b ORDER BY b
    SETTINGS optimize_use_projections = 0;
SELECT b, sum(a), count() FROM t_projection_filters GROUP BY b ORDER BY b
    SETTINGS optimize_aggregation_in_order = 0, parallel_replicas_local_plan = 1,
             parallel_replicas_support_projection = 1, force_optimize_projection = 1;

DROP TABLE t_projection_filters;
