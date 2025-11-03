SET mutations_sync = 2;

DROP TABLE IF EXISTS t_sparse_mutations_3;

CREATE TABLE t_sparse_mutations_3 (key UInt8, id UInt64, s String)
ENGINE = MergeTree ORDER BY id PARTITION BY key
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, serialization_info_version = 'basic';

INSERT INTO t_sparse_mutations_3 SELECT 1, number, toString(tuple(1, 0, '1', '0', '')) FROM numbers (10000);

SELECT type, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_3' AND column = 's' AND active
ORDER BY name;

ALTER TABLE t_sparse_mutations_3 MODIFY COLUMN s Tuple(UInt64, UInt64, String, String, String);

SELECT
    type,
    serialization_kind,
    subcolumns.names,
    subcolumns.types,
    subcolumns.serializations
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_3' AND column = 's' AND active
ORDER BY name;

SELECT sum(s.1), sum(s.2), groupUniqArray(s.3), groupUniqArray(s.4), groupUniqArray(s.5) FROM t_sparse_mutations_3;

OPTIMIZE TABLE t_sparse_mutations_3 FINAL;

SELECT
    type,
    serialization_kind,
    subcolumns.names,
    subcolumns.types,
    subcolumns.serializations
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_3' AND column = 's' AND active
ORDER BY name;

SELECT sum(s.1), sum(s.2), groupUniqArray(s.3), groupUniqArray(s.4), groupUniqArray(s.5) FROM t_sparse_mutations_3;

ALTER TABLE t_sparse_mutations_3 MODIFY COLUMN s Tuple(UInt64, UInt64, UInt64, UInt64, String);

SELECT
    type,
    serialization_kind,
    subcolumns.names,
    subcolumns.types,
    subcolumns.serializations
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_3' AND column = 's' AND active
ORDER BY name;

SELECT sum(s.1), sum(s.2), sum(s.3), sum(s.4), groupUniqArray(s.5) FROM t_sparse_mutations_3;

OPTIMIZE TABLE t_sparse_mutations_3 FINAL;

SELECT
    type,
    serialization_kind,
    subcolumns.names,
    subcolumns.types,
    subcolumns.serializations
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_3' AND column = 's' AND active
ORDER BY name;

SELECT sum(s.1), sum(s.2), sum(s.3), sum(s.4), groupUniqArray(s.5) FROM t_sparse_mutations_3;

SET mutations_sync=2;
ALTER TABLE t_sparse_mutations_3 MODIFY COLUMN s Tuple(Nullable(UInt64), Nullable(UInt64), Nullable(UInt64), Nullable(UInt64), Nullable(String));

SELECT
    type,
    serialization_kind,
    subcolumns.names,
    subcolumns.types,
    subcolumns.serializations
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_mutations_3' AND column = 's' AND active
ORDER BY name;

SELECT sum(s.1), sum(s.2), sum(s.3), sum(s.4), groupUniqArray(s.5) FROM t_sparse_mutations_3;

DROP TABLE t_sparse_mutations_3;
