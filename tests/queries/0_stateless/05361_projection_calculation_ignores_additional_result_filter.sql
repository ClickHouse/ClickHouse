-- A projection is calculated by an internal query over the block being written. The setting
-- `additional_result_filter` of the query which writes the data must not filter the result of that
-- internal query, otherwise only a part of the rows is persisted in the projection part.

DROP TABLE IF EXISTS t_projection_result_filter;
CREATE TABLE t_projection_result_filter
(
    a UInt64,
    b String,
    PROJECTION agg (SELECT b, sum(a), count() GROUP BY b),
    PROJECTION filtered (SELECT a, b WHERE a > 0 ORDER BY b)
)
ENGINE = MergeTree ORDER BY a;

SET additional_result_filter = 'b != \'0\'';

INSERT INTO t_projection_result_filter SELECT number, toString(number % 3) FROM numbers(10);
INSERT INTO t_projection_result_filter SELECT number, toString(number % 3) FROM numbers(10, 10);
OPTIMIZE TABLE t_projection_result_filter FINAL;

SET additional_result_filter = '';

-- The projections must give the same answer as the table itself.
SELECT b, sum(a), count() FROM t_projection_result_filter GROUP BY b ORDER BY b
    SETTINGS optimize_use_projections = 0;
SELECT b, sum(a), count() FROM t_projection_result_filter GROUP BY b ORDER BY b
    SETTINGS optimize_aggregation_in_order = 0, parallel_replicas_local_plan = 1,
             parallel_replicas_support_projection = 1, force_optimize_projection = 1;
SELECT b, count() FROM mergeTreeProjection(currentDatabase(), t_projection_result_filter, filtered) GROUP BY b ORDER BY b;

DROP TABLE t_projection_result_filter;
