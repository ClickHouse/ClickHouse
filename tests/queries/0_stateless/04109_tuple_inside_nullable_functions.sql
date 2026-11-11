-- { echo }

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS test_flatten_nullable_tuple;
CREATE TABLE test_flatten_nullable_tuple (id UInt32, data Nullable(Tuple(a UInt32, b Tuple(c String, d UInt32)))) ENGINE = Memory;
INSERT INTO test_flatten_nullable_tuple VALUES (1, (10, ('hello', 20))), (2, NULL), (3, (30, ('world', 40)));

SELECT id, flattenTuple(data) AS flat, isNull(flat) AS is_null, toTypeName(flat) AS type FROM test_flatten_nullable_tuple ORDER BY id;

SELECT flattenTuple(CAST((1, ('x', 2)) AS Tuple(a UInt32, b Tuple(c String, d UInt32)))) AS flat, toTypeName(flat);
SELECT flattenTuple(CAST(NULL AS Nullable(Tuple(a UInt32, b Tuple(c String, d UInt32))))) AS flat, toTypeName(flat);

DROP TABLE test_flatten_nullable_tuple;

DROP TABLE IF EXISTS test_name_value_pairs_nullable;
CREATE TABLE test_name_value_pairs_nullable (id UInt32, data Nullable(Tuple(a UInt32, b UInt32))) ENGINE = Memory;
INSERT INTO test_name_value_pairs_nullable VALUES (1, (10, 20)), (2, NULL), (3, (30, 40));

SELECT id, tupleToNameValuePairs(data) AS pairs, length(pairs) AS len, toTypeName(pairs) AS type FROM test_name_value_pairs_nullable ORDER BY id;

SELECT tupleToNameValuePairs(CAST((1, 2) AS Tuple(a UInt32, b UInt32))) AS pairs, toTypeName(pairs);
SELECT tupleToNameValuePairs(CAST(NULL AS Nullable(Tuple(a UInt32, b UInt32)))) AS pairs, toTypeName(pairs);

DROP TABLE test_name_value_pairs_nullable;

SELECT tupleToNameValuePairs(CAST((NULL, NULL) AS Tuple(a Nullable(UInt32), b Nullable(UInt32)))) AS pairs, toTypeName(pairs);
SELECT tupleToNameValuePairs(CAST((1, NULL) AS Tuple(a Nullable(UInt32), b Nullable(UInt32)))) AS pairs, toTypeName(pairs);

SELECT tupleToNameValuePairs(CAST((1, NULL) AS Nullable(Tuple(a Nullable(UInt32), b Nullable(UInt32))))) AS pairs, toTypeName(pairs);
SELECT tupleToNameValuePairs(CAST(NULL AS Nullable(Tuple(a Nullable(UInt32), b Nullable(UInt32))))) AS pairs, toTypeName(pairs);

SELECT tupleToNameValuePairs(CAST(('a', 'b') AS Tuple(a LowCardinality(String), b LowCardinality(String)))) AS pairs, toTypeName(pairs);
SELECT tupleToNameValuePairs(CAST(('a', 'b') AS Nullable(Tuple(a LowCardinality(String), b LowCardinality(String))))) AS pairs, toTypeName(pairs);
SELECT tupleToNameValuePairs(CAST(NULL AS Nullable(Tuple(a LowCardinality(String), b LowCardinality(String))))) AS pairs, toTypeName(pairs);

SELECT tupleToNameValuePairs(CAST(('x', 'y') AS Tuple(a LowCardinality(String), b String))) AS pairs, toTypeName(pairs);
SELECT tupleToNameValuePairs(CAST(('x', 'y') AS Nullable(Tuple(a LowCardinality(String), b String)))) AS pairs, toTypeName(pairs);
SELECT tupleToNameValuePairs(CAST(NULL AS Nullable(Tuple(a LowCardinality(String), b String)))) AS pairs, toTypeName(pairs);

SELECT tupleToNameValuePairs(CAST((1, 2) AS Tuple(a UInt32, b UInt64))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tupleToNameValuePairs(CAST((1, NULL) AS Tuple(a UInt32, b Nullable(UInt32)))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT tupleToNameValuePairs(CAST(([1,2], [3,4]) AS Tuple(a Array(UInt32), b Array(UInt32)))) AS pairs, toTypeName(pairs);
SELECT tupleToNameValuePairs(CAST(([1,2], [3,4]) AS Nullable(Tuple(a Array(UInt32), b Array(UInt32))))) AS pairs, toTypeName(pairs);
SELECT tupleToNameValuePairs(CAST(NULL AS Nullable(Tuple(a Array(UInt32), b Array(UInt32))))) AS pairs, toTypeName(pairs);
