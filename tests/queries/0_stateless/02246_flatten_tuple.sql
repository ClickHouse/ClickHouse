-- Tags: no-fasttest

DROP TABLE IF EXISTS t_flatten_tuple;
DROP TABLE IF EXISTS t_flatten_object;

SET flatten_nested = 0;

CREATE TABLE t_flatten_tuple(t Tuple(t1 Nested(a UInt32, s String), b UInt32, t2 Tuple(k String, v UInt32))) ENGINE = Memory;

INSERT INTO t_flatten_tuple VALUES (([(1, 'a'), (2, 'b')], 3, ('c', 4)));

SELECT flattenTuple(t) AS ft, toTypeName(ft) FROM t_flatten_tuple;

DROP TABLE IF EXISTS t_flatten_tuple;
