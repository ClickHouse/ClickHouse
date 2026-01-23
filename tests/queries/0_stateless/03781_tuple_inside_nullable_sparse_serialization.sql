-- Sparse serialization for Nullable(Tuple) columns not supported yet

SET enable_multiple_prewhere_read_steps = 0;

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS nullable_tuple_sparse;

CREATE TABLE nullable_tuple_sparse
(
    tup Nullable(Tuple(u UInt64, s String))
)
ENGINE = MergeTree
SETTINGS ratio_of_defaults_for_sparse_serialization = 0, nullable_serialization_version = 'allow_sparse', min_bytes_for_wide_part = 0;


INSERT INTO nullable_tuple_sparse (tup) VALUES (NULL);

SELECT tup FROM nullable_tuple_sparse
WHERE tup IS NULL AND tup.s = 'a';

DROP TABLE IF EXISTS nullable_tuple_sparse_2;

CREATE TABLE nullable_tuple_sparse_2
(
    tup Nullable(Tuple(u UInt64, s String))
)
ENGINE = MergeTree
SETTINGS ratio_of_defaults_for_sparse_serialization = 0, nullable_serialization_version = 'allow_sparse', min_bytes_for_wide_part = 0;

INSERT INTO nullable_tuple_sparse_2
SELECT number % 5 == 0 ? (number, toString(number)) : NULL
FROM numbers(1000);

SELECT sum(not isNull(tup)) FROM nullable_tuple_sparse_2;
SELECT sum(isNull(tup)) FROM nullable_tuple_sparse_2;

SELECT
    sum(toUInt64(isNull(tup.s))) AS null_s,
    sum(toUInt64(isNull(tup.u))) AS null_u
FROM nullable_tuple_sparse_2;

SELECT count() FROM nullable_tuple_sparse_2 WHERE tup IS NULL AND tup.s IS NULL;
SELECT count() FROM nullable_tuple_sparse_2 WHERE tup.s = '10';
SELECT sum(tup.u) FROM nullable_tuple_sparse_2 WHERE NOT isNull(tup);

SELECT tup.u, tup.s
FROM nullable_tuple_sparse_2
WHERE tup.u IN (0, 5, 10)
ORDER BY tup.u;

DROP TABLE IF EXISTS test_structure;

CREATE TABLE test_structure (
    t Nullable(Tuple(x UInt32, y UInt64)),
    PRIMARY KEY ()
) ENGINE = MergeTree
SETTINGS ratio_of_defaults_for_sparse_serialization = 0, nullable_serialization_version = 'allow_sparse', min_bytes_for_wide_part = 0;

INSERT INTO test_structure
SELECT number % 3 == 0 ? (number, number * 123456789) : NULL
FROM numbers(10);

SELECT DISTINCT dumpColumnStructure(*) FROM test_structure;

DROP TABLE IF EXISTS test_structure_2;

CREATE TABLE test_structure_2 (
    t Nullable(Tuple(x UInt32, y UInt64)),
    PRIMARY KEY ()
) ENGINE = MergeTree
SETTINGS ratio_of_defaults_for_sparse_serialization = 0, nullable_serialization_version = 'allow_sparse', min_bytes_for_wide_part = 0;

INSERT INTO test_structure_2
SELECT number % 3 == 0 ? (number, number * 123456789) : (0, 0)
FROM numbers(10);

SELECT DISTINCT dumpColumnStructure(*) FROM test_structure_2;
