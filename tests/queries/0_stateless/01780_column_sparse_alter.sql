
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_sparse_alter;

CREATE TABLE t_sparse_alter (id UInt64, u UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO t_sparse_alter SELECT
    number,
    if (number % 11 = 0, number, 0),
    if (number % 13 = 0, toString(number), '')
FROM numbers(2000);

SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sparse_alter' AND active ORDER BY column;

SELECT uniqExact(u), uniqExact(s) FROM t_sparse_alter;

ALTER TABLE t_sparse_alter DROP COLUMN s, RENAME COLUMN u TO t;
ALTER TABLE t_sparse_alter MODIFY COLUMN t UInt16;

SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sparse_alter' AND active ORDER BY column;

SELECT uniqExact(t) FROM t_sparse_alter;

DETACH TABLE t_sparse_alter;
ATTACH TABLE t_sparse_alter;

SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sparse_alter' AND active ORDER BY column;
SELECT uniqExact(t) FROM t_sparse_alter;

DROP TABLE t_sparse_alter;
