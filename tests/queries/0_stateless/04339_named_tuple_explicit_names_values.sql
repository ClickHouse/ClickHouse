-- Tests for the explicit named tuple syntax tuple(name1, name2, ...)(value1, value2, ...).

-- Two tuples with the same values but different element names must not collide by column name.
SELECT tuple('a')(1) AS x, tuple('b')(1) AS y, toTypeName(x), toTypeName(y);
SELECT tuple('a', 'b')(1, 2) AS t, toTypeName(t);

-- In VALUES, an interpreted named tuple expression with the same set of element names as the
-- destination must be converted by name, not by position.
DROP TABLE IF EXISTS t_named_tuple_values;
CREATE TABLE t_named_tuple_values (t Tuple(a Int32, b Int32)) ENGINE = Memory;
INSERT INTO t_named_tuple_values VALUES (tuple('b', 'a')(1, 2));
SELECT t.a, t.b FROM t_named_tuple_values;
DROP TABLE t_named_tuple_values;
