SET mutations_sync = 2;

DROP TABLE IF EXISTS t_sparse_mutations_2;

CREATE TABLE t_sparse_mutations_2 (key UInt8, id UInt64, s String)
ENGINE = MergeTree ORDER BY id PARTITION BY key
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_sparse_mutations_2 SELECT 1, number, toString(number) FROM numbers (10000);

SELECT type, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_2' AND column = 's' AND active
ORDER BY name;

SELECT count(), sum(s::UInt64) FROM t_sparse_mutations_2 WHERE s != '';

ALTER TABLE t_sparse_mutations_2 UPDATE s = '' WHERE id % 13 != 0;

SELECT type, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_2' AND column = 's' AND active
ORDER BY name;

SELECT count(), sum(s::UInt64) FROM t_sparse_mutations_2 WHERE s != '';

OPTIMIZE TABLE t_sparse_mutations_2 FINAL;

SELECT type, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_2' AND column = 's' AND active
ORDER BY name;

SELECT count(), sum(s::UInt64) FROM t_sparse_mutations_2 WHERE s != '';

DROP TABLE t_sparse_mutations_2;
