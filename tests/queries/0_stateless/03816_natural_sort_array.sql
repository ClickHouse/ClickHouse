-- Transitive NATURAL for Array: ORDER BY arr NATURAL compares element-wise with natural for string elements.

DROP TABLE IF EXISTS t_natural_array;
CREATE TABLE t_natural_array(arr Array(String)) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO t_natural_array VALUES (['item10', 'a']), (['item2', 'b']), (['item1', 'c']);
SELECT * FROM t_natural_array ORDER BY arr NATURAL;
DROP TABLE IF EXISTS t_natural_array;
