DROP TABLE IF EXISTS t_sparse;

CREATE TABLE t_sparse (id UInt64, u UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_sparse SELECT
    number,
    if (number % 20 = 0, number, 0),
    if (number % 50 = 0, toString(number), '')
FROM numbers(1, 100000);

SELECT column, serialization_kind FROM system.parts_columns
WHERE table = 't_sparse' AND database = currentDatabase()
ORDER BY column, serialization_kind;

SELECT count() FROM t_sparse WHERE u > 0;
SELECT count() FROM t_sparse WHERE notEmpty(s);

SYSTEM STOP MERGES t_sparse;

INSERT INTO t_sparse SELECT
    number, number, toString(number)
FROM numbers (1, 100000);

SELECT column, serialization_kind FROM system.parts_columns
WHERE table = 't_sparse' AND database = currentDatabase()
ORDER BY column, serialization_kind;

SELECT count() FROM t_sparse WHERE u > 0;
SELECT count() FROM t_sparse WHERE notEmpty(s);

DROP TABLE t_sparse;
