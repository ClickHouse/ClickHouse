-- Transitive NATURAL for Tuple: ORDER BY tuple_col NATURAL compares component-wise with natural for string-like components.

DROP TABLE IF EXISTS t_natural_tuple;
CREATE TABLE t_natural_tuple(t Tuple(String)) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO t_natural_tuple VALUES (('item10',)), (('item2',)), (('item1',));
SELECT * FROM t_natural_tuple ORDER BY t NATURAL;
DROP TABLE IF EXISTS t_natural_tuple;
