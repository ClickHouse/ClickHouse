SET mutations_sync = 2;

DROP TABLE IF EXISTS t_sparse_mutations_4;

CREATE TABLE t_sparse_mutations_4 (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_sparse_mutations_4 SELECT number, 0 FROM numbers(10000);

SELECT type, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_4' AND column = 'v' AND active
ORDER BY name;

ALTER TABLE t_sparse_mutations_4 MODIFY COLUMN v String;

SELECT type, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_4' AND column = 'v' AND active
ORDER BY name;

DROP TABLE t_sparse_mutations_4;
