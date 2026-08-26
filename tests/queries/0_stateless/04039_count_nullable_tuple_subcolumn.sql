-- { echo }

SET allow_experimental_nullable_tuple_type = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS test;
CREATE TABLE test (t Tuple(v Nullable(Tuple(w Nullable(UInt32))))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test VALUES (((1))), (((NULL))), ((NULL));

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count(t.v.w) FROM test SETTINGS enable_analyzer = 1;
SELECT count(t.v.w) FROM test;

DROP TABLE IF EXISTS test2;
CREATE TABLE test2 (t Nullable(Tuple(u Nullable(UInt32)))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test2 VALUES ((1)), ((NULL)), (NULL);

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count(t.u) FROM test2 SETTINGS enable_analyzer = 1;
SELECT count(t.u) FROM test2;

-- Test from https://github.com/ClickHouse/ClickHouse/pull/99490
DROP TABLE IF EXISTS t_nullable_tuple;

CREATE TABLE t_nullable_tuple
(
    `tup` Nullable(Tuple(u UInt64, s Nullable(String)))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0, nullable_serialization_version = 'allow_sparse', min_bytes_for_wide_part = 0;

INSERT INTO t_nullable_tuple SELECT if((number % 5) = 0, (number, toString(number)), NULL) FROM numbers(1000);

SELECT count(tup.u) FROM t_nullable_tuple SETTINGS optimize_functions_to_subcolumns = 1;

-- TODO: These output 1000 but should be 200. This is wrong but is a different bug.
SELECT count(tup.s) FROM t_nullable_tuple SETTINGS optimize_functions_to_subcolumns = 1;
SELECT DISTINCT count(tup.s) FROM t_nullable_tuple SETTINGS optimize_functions_to_subcolumns = 1;
