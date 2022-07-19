DROP TABLE IF EXISTS t_sparse_distinct;

CREATE TABLE t_sparse_distinct (id UInt32, v UInt64)
ENGINE = MergeTree
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

SYSTEM STOP MERGES t_sparse_distinct;

INSERT INTO t_sparse_distinct SELECT number, number % 5 FROM numbers(100000);
INSERT INTO t_sparse_distinct SELECT number, number % 100 = 0 FROM numbers(100000);

SELECT name, column, serialization_kind
FROM system.parts_columns
WHERE table = 't_sparse_distinct' AND database = currentDatabase() AND column = 'v'
ORDER BY name;

SELECT DISTINCT v FROM t_sparse_distinct ORDER BY v;

DROP TABLE t_sparse_distinct;
