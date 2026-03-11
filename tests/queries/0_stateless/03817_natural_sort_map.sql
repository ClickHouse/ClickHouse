-- Transitive NATURAL for Map: ORDER BY map_col NATURAL compares via nested Array(Tuple(key, value)), natural for string keys/values.

DROP TABLE IF EXISTS t_natural_map;
CREATE TABLE t_natural_map(m Map(String, Int64)) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO t_natural_map VALUES (map('item10', 1, 'item2', 2)), (map('item1', 3));
SELECT * FROM t_natural_map ORDER BY m NATURAL;
DROP TABLE IF EXISTS t_natural_map;
