SET mutations_sync = 2;

DROP TABLE IF EXISTS t_sparse_mutations_1;

CREATE TABLE t_sparse_mutations_1 (key UInt8, id UInt64, s String)
ENGINE = MergeTree ORDER BY id PARTITION BY key
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_sparse_mutations_1 SELECT 1, number, if (number % 21 = 0, 'foo', '') FROM numbers (10000);

SELECT name, type, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_1' AND column = 's' AND active
ORDER BY name;

SELECT countIf(s = 'foo'), arraySort(groupUniqArray(s)) FROM t_sparse_mutations_1;

ALTER TABLE t_sparse_mutations_1 MODIFY COLUMN s Nullable(String);

SELECT name, type, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_1' AND column = 's' AND active
ORDER BY name;

SELECT countIf(s = 'foo'), arraySort(groupUniqArray(s)) FROM t_sparse_mutations_1;

INSERT INTO t_sparse_mutations_1 SELECT 2, number, if (number % 21 = 0, 'foo', '') FROM numbers (10000);

SELECT name, type, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_1' AND column = 's' AND active
ORDER BY name;

SELECT countIf(s = 'foo'), arraySort(groupUniqArray(s)) FROM t_sparse_mutations_1;

ALTER TABLE t_sparse_mutations_1 MODIFY COLUMN s String;

SELECT name, type, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_1' AND column = 's' AND active
ORDER BY name;

SELECT countIf(s = 'foo'), arraySort(groupUniqArray(s)) FROM t_sparse_mutations_1;

OPTIMIZE TABLE t_sparse_mutations_1 FINAL;

SELECT name, type, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_1' AND column = 's' AND active
ORDER BY name;

SELECT countIf(s = 'foo'), arraySort(groupUniqArray(s)) FROM t_sparse_mutations_1;

DROP TABLE t_sparse_mutations_1;
