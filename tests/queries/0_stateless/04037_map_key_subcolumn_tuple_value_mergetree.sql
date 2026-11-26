-- Tags: no-random-settings

-- Reading Map key subcolumn with Tuple value type from MergeTree compact parts
-- previously caused an exception because `SerializationMapKeyValue` wrapped the
-- Array(Tuple(K,V)) serialization, and `DataTypeTuple::createColumn` expected
-- a `SerializationTuple` after unwrapping.

DROP TABLE IF EXISTS t_map_tuple;

CREATE TABLE t_map_tuple (id UInt64, m Map(String, Tuple(UInt64, String))) ENGINE = MergeTree() ORDER BY id;
INSERT INTO t_map_tuple VALUES (1, {'a' : (1, 'x'), 'b' : (2, 'y')}), (2, {'c' : (10, 'hello')}), (3, {}), (4, {'a' : (0, ''), 'b' : (99, 'end')});

SELECT m.key_a FROM t_map_tuple ORDER BY id;
SELECT m.key_b FROM t_map_tuple ORDER BY id;
SELECT m.key_c FROM t_map_tuple ORDER BY id;
SELECT m.key_missing FROM t_map_tuple ORDER BY id;

DROP TABLE t_map_tuple;
