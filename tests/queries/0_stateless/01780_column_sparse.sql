-- { echo }

DROP TABLE IF EXISTS t_sparse;
DROP TABLE IF EXISTS t_sparse_1;

CREATE TABLE t_sparse (id UInt64, u UInt64, s String, arr1 Array(String), arr2 Array(UInt64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;

INSERT INTO t_sparse SELECT
    number,
    if (number % 10 = 0, number, 0),
    if (number % 5 = 0, toString(number), ''),
    if (number % 7 = 0, arrayMap(x -> toString(x), range(number % 10)), []),
    if (number % 12 = 0, range(number % 10), [])
FROM numbers (200);

SELECT column, serialization_kind FROM system.parts_columns
WHERE table = 't_sparse' AND database = currentDatabase()
ORDER BY column;

SELECT * FROM t_sparse WHERE u != 0 ORDER BY id;
SELECT * FROM t_sparse WHERE s != '' ORDER BY id;
SELECT * FROM t_sparse WHERE arr1 != [] ORDER BY id;
SELECT * FROM t_sparse WHERE arr2 != [] ORDER BY id;

SELECT sum(u) FROM t_sparse;
SELECT sum(u) FROM t_sparse GROUP BY id % 7;

SELECT arrayFilter(x -> x % 2 = 1, arr2) FROM t_sparse WHERE arr2 != [] LIMIT 5;

CREATE TABLE t_sparse_1 (id UInt64, v Int64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0;

INSERT INTO t_sparse_1 VALUES (1, 6), (2, 1), (3, 0), (4, -1), (5, 0), (6, 0), (7, -2), (8, 0), (9, 0), (10, 4), (11, 0);

SELECT * FROM t_sparse_1 ORDER BY v, id;
SELECT * FROM t_sparse_1 ORDER BY v DESC, id;
SELECT * FROM t_sparse_1 ORDER BY v, id LIMIT 5;
SELECT * FROM t_sparse_1 ORDER BY v DESC, id LIMIT 5;

DROP TABLE t_sparse;
DROP TABLE t_sparse_1;
