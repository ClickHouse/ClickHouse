DROP TABLE IF EXISTS t_sparse_mutation;

CREATE TABLE t_sparse_mutation (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_sparse_mutation select number, if (number % 21 = 0, number, 0) FROM numbers(10000);

SET mutations_sync = 2;

DELETE FROM t_sparse_mutation WHERE id % 2 = 0;

SELECT count(), sum(v) FROM t_sparse_mutation;

SELECT sum(has_lightweight_delete) FROM system.parts
WHERE database = currentDatabase() AND table = 't_sparse_mutation' AND active;

ALTER TABLE t_sparse_mutation UPDATE v = v * 2 WHERE id % 5 = 0;
ALTER TABLE t_sparse_mutation DELETE WHERE id % 3 = 0;

SELECT count(), sum(v) FROM t_sparse_mutation;

OPTIMIZE TABLE t_sparse_mutation FINAL;

SELECT sum(has_lightweight_delete) FROM system.parts
WHERE database = currentDatabase() AND table = 't_sparse_mutation' AND active;

SELECT count(), sum(v) FROM t_sparse_mutation;

DROP TABLE t_sparse_mutation;
