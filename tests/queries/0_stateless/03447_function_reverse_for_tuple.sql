SELECT reverse((1, 'Hello', [2, 3]));

DROP TABLE IF EXISTS t_tuple;

CREATE TABLE t_tuple(tuple Tuple(a Int32, b String)) engine = MergeTree order by tuple();

INSERT INTO t_tuple VALUES((1, 'hello')), ((2, 'world')), ((3, 'clickhouse'));

SELECT reverse(tuple) FROM t_tuple;
SELECT reverse(tuple).a, reverse(tuple).b FROM t_tuple;

DROP TABLE t_tuple;
