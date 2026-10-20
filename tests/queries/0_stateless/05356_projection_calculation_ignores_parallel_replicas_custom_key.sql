-- A projection is calculated over the whole block being written. A context which carries the settings of
-- a replica of `parallel_replicas_custom_key` must not split the source of the calculation between the
-- replicas, otherwise only a slice of the rows is persisted in the projection part.

DROP TABLE IF EXISTS t_projection_custom_key;
CREATE TABLE t_projection_custom_key (a UInt64, b String, PROJECTION p (SELECT b, sum(a), count() GROUP BY b))
ENGINE = MergeTree ORDER BY a SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_custom_key
SETTINGS max_parallel_replicas = 2, allow_experimental_parallel_reading_from_replicas = 1,
         parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'a',
         parallel_replicas_count = 2, parallel_replica_offset = 1
VALUES (0, '0'), (1, '1'), (2, '0'), (0, '1'), (1, '0'), (2, '1'), (0, '0'), (1, '1'), (2, '0'), (0, '1');

-- The projection must give the same answer as the table itself.
SELECT b, sum(a), count() FROM t_projection_custom_key GROUP BY b ORDER BY b
    SETTINGS optimize_use_projections = 0;
SELECT b, sum(a), count() FROM t_projection_custom_key GROUP BY b ORDER BY b
    SETTINGS optimize_aggregation_in_order = 0, parallel_replicas_local_plan = 1,
             parallel_replicas_support_projection = 1, force_optimize_projection = 1;

DROP TABLE t_projection_custom_key;
