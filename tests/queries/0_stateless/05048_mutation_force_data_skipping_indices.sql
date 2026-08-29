-- The check query and the index-analysis fast path of a mutation must not honor
-- force_data_skipping_indices, in the same way they ignore force_index_by_date and force_primary_key.

DROP TABLE IF EXISTS t_mutation_force_idx;

CREATE TABLE t_mutation_force_idx (p UInt8, id UInt64, v UInt64, INDEX idx v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree PARTITION BY p ORDER BY id;

INSERT INTO t_mutation_force_idx SELECT 0, number, number FROM numbers(10);
INSERT INTO t_mutation_force_idx SELECT 1, number, number FROM numbers(10);

-- Partition pruning proves the part of partition 1 untouched; the part of partition 0 is mutated.
ALTER TABLE t_mutation_force_idx DELETE WHERE p = 0 AND id < 5 SETTINGS mutations_sync = 2, force_data_skipping_indices = 'idx';
SELECT p, count() FROM t_mutation_force_idx GROUP BY p ORDER BY p;

-- Empty forced index list used to fail with CANNOT_PARSE_TEXT in the check query.
ALTER TABLE t_mutation_force_idx UPDATE v = v + 100 WHERE id >= 8 SETTINGS mutations_sync = 2, force_data_skipping_indices = '';
SELECT p, sum(v) FROM t_mutation_force_idx GROUP BY p ORDER BY p;

DROP TABLE t_mutation_force_idx;
