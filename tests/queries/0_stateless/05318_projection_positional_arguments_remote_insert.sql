-- A projection whose `GROUP BY` uses positional arguments must be calculated correctly when the data
-- comes from a remote `INSERT`, which is executed on the receiving server as a `SECONDARY_QUERY`.
-- The Analyzer does not resolve positional arguments of a secondary query by itself, because it
-- expects the initiator to have done that, but nobody resolves them for a projection query.

SET enable_positional_arguments_for_projections = 1;

DROP TABLE IF EXISTS t_projection_positional_remote;

CREATE TABLE t_projection_positional_remote (a UInt64, b String)
ENGINE = MergeTree ORDER BY a
SETTINGS materialize_projections_on_insert = 1;

ALTER TABLE t_projection_positional_remote ADD PROJECTION p (SELECT b, sum(a), count() GROUP BY 1);

INSERT INTO FUNCTION remote('127.0.0.2', currentDatabase(), t_projection_positional_remote)
SELECT number % 7, toString(number % 3) FROM numbers(20)
SETTINGS distributed_foreground_insert = 1, prefer_localhost_replica = 0, async_insert = 0;

SELECT arraySort(groupArray(DISTINCT column)) FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_positional_remote' AND name = 'p' AND active;

SELECT b, sum(a), count() FROM t_projection_positional_remote GROUP BY b ORDER BY b SETTINGS optimize_use_projections = 0;

SET optimize_aggregation_in_order = 0, parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1;
SELECT b, sum(a), count() FROM t_projection_positional_remote GROUP BY b ORDER BY b SETTINGS force_optimize_projection = 1;

DROP TABLE t_projection_positional_remote;
