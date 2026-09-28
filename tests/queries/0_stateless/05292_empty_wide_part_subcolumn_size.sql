-- Random settings limits: optimize_functions_to_subcolumns=(1, 1); optimize_move_to_prewhere=(1, 1); query_plan_optimize_prewhere=(1, 1); allow_calculating_subcolumns_sizes_for_merge_tree_reading=(1, 1)

-- An active Wide part without rows, left here by a mutation that deleted all its rows, must not break
-- the subcolumn size estimate of the PREWHERE optimization.

DROP TABLE IF EXISTS t_empty_part;

CREATE TABLE t_empty_part (p UInt8, m Map(LowCardinality(String), String))
ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, remove_empty_parts = 0;

INSERT INTO t_empty_part VALUES (1, {'k': 'v'});
ALTER TABLE t_empty_part DELETE WHERE p = 1 SETTINGS mutations_sync = 2;
INSERT INTO t_empty_part VALUES (2, {'k': 'v'});
ALTER TABLE t_empty_part MODIFY SETTING min_bytes_for_wide_part = 1000000000;
INSERT INTO t_empty_part VALUES (3, {'k': 'v'});

SELECT partition, part_type, rows FROM system.parts
WHERE database = currentDatabase() AND table = 't_empty_part' AND active
ORDER BY partition;

-- Only a Compact part is left after partition pruning.
SELECT count() FROM t_empty_part WHERE p = 3 AND m['k'] = 'v';

-- Automatic parallel replicas.
SELECT count() FROM t_empty_part WHERE m['k'] = 'v'
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    automatic_parallel_replicas_min_bytes_per_replica = 0, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE t_empty_part;
