SET mutations_sync = 2;

DROP TABLE IF EXISTS t_sparse_mutations_5;

CREATE TABLE t_sparse_mutations_5 (k UInt64, t Tuple(UInt64, UInt64))
ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, serialization_info_version = 'basic';

INSERT INTO t_sparse_mutations_5 SELECT number, (0, 0) FROM numbers(10000);

SELECT type, serialization_kind, subcolumns.names, subcolumns.types, subcolumns.serializations FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_5' AND column = 't' AND active
ORDER BY name;

ALTER TABLE t_sparse_mutations_5 MODIFY COLUMN t Tuple(UInt64, String);

SELECT type, serialization_kind, subcolumns.names, subcolumns.types, subcolumns.serializations FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_5' AND column = 't' AND active
ORDER BY name;

DROP TABLE t_sparse_mutations_5;
