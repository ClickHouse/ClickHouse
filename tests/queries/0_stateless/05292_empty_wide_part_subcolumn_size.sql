-- Random settings limits: optimize_functions_to_subcolumns=(1, 1); optimize_move_to_prewhere=(1, 1); query_plan_optimize_prewhere=(1, 1); allow_calculating_subcolumns_sizes_for_merge_tree_reading=(1, 1)

-- An active part without rows, left here by a mutation that deleted all its rows or by a TTL drop, must not break
-- the subcolumn size estimate of the PREWHERE optimization, for Wide (full and packed storage) and Compact parts.

DROP TABLE IF EXISTS t_empty_wide;
DROP TABLE IF EXISTS t_empty_packed;
DROP TABLE IF EXISTS t_empty_compact;
DROP TABLE IF EXISTS t_empty_ttl;

CREATE TABLE t_empty_wide
(
    p UInt8,
    m Map(LowCardinality(String), String),
    mv Map(String, LowCardinality(String)),
    t Tuple(a LowCardinality(String), b UInt64),
    v Variant(String, UInt64),
    d Dynamic,
    j JSON
)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, remove_empty_parts = 0;

CREATE TABLE t_empty_packed AS t_empty_wide ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 1000000000, remove_empty_parts = 0;

CREATE TABLE t_empty_compact AS t_empty_wide ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0, remove_empty_parts = 0;

CREATE TABLE t_empty_ttl (p UInt8, ts DateTime, m Map(LowCardinality(String), String))
ENGINE = MergeTree PARTITION BY p ORDER BY tuple() TTL ts
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, remove_empty_parts = 0,
    ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0;

INSERT INTO t_empty_wide VALUES (1, {'k': 'v'}, {'k': 'v'}, ('v', 1), 'v', 'v', '{"a": "v"}');
ALTER TABLE t_empty_wide DELETE WHERE p = 1 SETTINGS mutations_sync = 2;
INSERT INTO t_empty_wide VALUES (2, {'k': 'v'}, {'k': 'v'}, ('v', 1), 'v', 'v', '{"a": "v"}');
ALTER TABLE t_empty_wide MODIFY SETTING min_bytes_for_wide_part = 1000000000;
INSERT INTO t_empty_wide VALUES (3, {'k': 'v'}, {'k': 'v'}, ('v', 1), 'v', 'v', '{"a": "v"}');

INSERT INTO t_empty_packed VALUES (1, {'k': 'v'}, {'k': 'v'}, ('v', 1), 'v', 'v', '{"a": "v"}');
ALTER TABLE t_empty_packed DELETE WHERE p = 1 SETTINGS mutations_sync = 2;
INSERT INTO t_empty_packed VALUES (2, {'k': 'v'}, {'k': 'v'}, ('v', 1), 'v', 'v', '{"a": "v"}');
ALTER TABLE t_empty_packed MODIFY SETTING min_bytes_for_wide_part = 1000000000;
INSERT INTO t_empty_packed VALUES (3, {'k': 'v'}, {'k': 'v'}, ('v', 1), 'v', 'v', '{"a": "v"}');

INSERT INTO t_empty_compact VALUES (1, {'k': 'v'}, {'k': 'v'}, ('v', 1), 'v', 'v', '{"a": "v"}');
ALTER TABLE t_empty_compact DELETE WHERE p = 1 SETTINGS mutations_sync = 2;
INSERT INTO t_empty_compact VALUES (2, {'k': 'v'}, {'k': 'v'}, ('v', 1), 'v', 'v', '{"a": "v"}');
INSERT INTO t_empty_compact VALUES (3, {'k': 'v'}, {'k': 'v'}, ('v', 1), 'v', 'v', '{"a": "v"}');

SET optimize_on_insert = 0;
INSERT INTO t_empty_ttl VALUES (1, '2000-01-01', {'k': 'v'});
SET optimize_on_insert = 1;
-- Keep the part written by the TTL drop instead of merging it again.
OPTIMIZE TABLE t_empty_ttl PARTITION 1 FINAL SETTINGS optimize_skip_merged_partitions = 1;
INSERT INTO t_empty_ttl VALUES (2, '2100-01-01', {'k': 'v'});
ALTER TABLE t_empty_ttl MODIFY SETTING min_bytes_for_wide_part = 1000000000;
INSERT INTO t_empty_ttl VALUES (3, '2100-01-01', {'k': 'v'});

SELECT table, partition, part_type, part_storage_type, rows FROM system.parts
WHERE database = currentDatabase() AND table LIKE 't_empty_%' AND active
ORDER BY table, partition;

-- Only a Compact part is left after partition pruning.
SELECT count() FROM t_empty_wide WHERE p = 3 AND m['k'] = 'v';
SELECT count() FROM t_empty_wide WHERE p = 3 AND mv['k'] = 'v';
SELECT count() FROM t_empty_wide WHERE p = 3 AND t.a = 'v';
SELECT count() FROM t_empty_wide WHERE p = 3 AND v.String = 'v';
SELECT count() FROM t_empty_wide WHERE p = 3 AND d.String = 'v';
SELECT count() FROM t_empty_wide WHERE p = 3 AND j.a::String = 'v';

SELECT count() FROM t_empty_packed WHERE p = 3 AND m['k'] = 'v';
SELECT count() FROM t_empty_packed WHERE p = 3 AND mv['k'] = 'v';
SELECT count() FROM t_empty_packed WHERE p = 3 AND t.a = 'v';
SELECT count() FROM t_empty_packed WHERE p = 3 AND v.String = 'v';
SELECT count() FROM t_empty_packed WHERE p = 3 AND d.String = 'v';
SELECT count() FROM t_empty_packed WHERE p = 3 AND j.a::String = 'v';

SELECT count() FROM t_empty_compact WHERE p = 3 AND m['k'] = 'v';
SELECT count() FROM t_empty_compact WHERE p = 3 AND mv['k'] = 'v';
SELECT count() FROM t_empty_compact WHERE p = 3 AND t.a = 'v';
SELECT count() FROM t_empty_compact WHERE p = 3 AND v.String = 'v';
SELECT count() FROM t_empty_compact WHERE p = 3 AND d.String = 'v';
SELECT count() FROM t_empty_compact WHERE p = 3 AND j.a::String = 'v';

SELECT count() FROM t_empty_ttl WHERE p = 3 AND m['k'] = 'v';

-- Automatic parallel replicas.
SELECT count() FROM t_empty_wide WHERE m['k'] = 'v'
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    automatic_parallel_replicas_min_bytes_per_replica = 0, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE t_empty_wide;
DROP TABLE t_empty_packed;
DROP TABLE t_empty_compact;
DROP TABLE t_empty_ttl;
