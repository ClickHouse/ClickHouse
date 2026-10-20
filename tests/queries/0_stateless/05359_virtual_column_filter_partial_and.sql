-- Random settings limits: merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=(0, 0)
-- `PartsSplitter` fault injection reads the granule on each layer border twice, which breaks the `max_rows_to_read` bound.
-- Pruning parts by virtual columns keeps the usable conjuncts of an AND; the reduced AND must still be executable lazily.

DROP TABLE IF EXISTS t_05359;
CREATE TABLE t_05359 (p UInt8, id UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY id;
INSERT INTO t_05359 SELECT number % 4, number FROM numbers(400);
SET max_rows_to_read = 300; -- partition 0 must be pruned by the virtual-column filter

SELECT count() FROM t_05359 WHERE _partition_id = '3' OR (id < 1000 AND _partition_id != '0' AND intDiv(10, length(_partition_id)) > 0);
SELECT count() FROM t_05359 WHERE _partition_id = '3' OR (id < 1000 AND _partition_id != '0' AND length(_partition_id) = 1)
SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT count() FROM t_05359 WHERE _partition_id = '3' OR (toNullable(id) < 1000 AND _partition_id != '0' AND intDiv(10, length(_partition_id)) > 0);

DROP TABLE t_05359;
