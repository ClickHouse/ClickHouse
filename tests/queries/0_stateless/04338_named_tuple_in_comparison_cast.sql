-- Implicit comparison casts of named tuples (the left hand side of IN is cast to the type of the
-- set elements) match the elements positionally, regardless of element names - consistently with
-- the comparison functions such as `equals`. Explicit CAST and accurateCast keep matching the
-- elements of named tuples by name.

DROP TABLE IF EXISTS t_named_tuple_in;

CREATE TABLE t_named_tuple_in (id UInt64, str String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_named_tuple_in SELECT number, concat('Hello', toString(number)) FROM numbers(100);

-- Different arity: an error, not a match of default values (when both sides were named tuples,
-- this used to drop both source fields, fill `number` with its default and match all 100 rows).
SELECT count() FROM t_named_tuple_in WHERE (id, str) IN (SELECT tuple(number) FROM numbers(100)); -- { serverError TYPE_MISMATCH, NUMBER_OF_COLUMNS_DOESNT_MATCH, ILLEGAL_TYPE_OF_ARGUMENT }

-- Same arity, different names: positional conversion, as for unnamed tuples.
SELECT count() FROM t_named_tuple_in WHERE (id, str) IN (
    SELECT tuple(x, y) FROM (SELECT number AS x, concat('Hello', toString(number)) AS y FROM numbers(10) WHERE number = 5));

-- Same set of names in a different order: the comparison stays positional
-- (names do not reorder the elements in comparisons, same as for `equals`).
SELECT (1 AS a, 2 AS b) IN (SELECT tuple('b', 'a')(1, 2));
SELECT (1 AS a, 2 AS b) IN (SELECT tuple('b', 'a')(2, 1));

-- The same on a table with a primary key: the index analysis must agree with the row-level comparison.
DROP TABLE IF EXISTS t_named_tuple_pk;
CREATE TABLE t_named_tuple_pk (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_named_tuple_pk VALUES (1, 2);
SELECT count() FROM t_named_tuple_pk WHERE (a, b) IN (SELECT tuple('b', 'a')(1, 2));
SELECT count() FROM t_named_tuple_pk WHERE (a, b) IN (SELECT tuple('b', 'a')(2, 1));
DROP TABLE t_named_tuple_pk;

-- Explicit CAST keeps the permissive named tuple semantics: the elements are matched by name,
-- extra source fields are dropped and missing destination fields are filled with default values.
SELECT (1 AS a, 2 AS b)::Tuple(a UInt32, c UInt32);
SELECT accurateCast(tuple('b', 'a')(1, 2), 'Tuple(a Int8, b Int8)');

DROP TABLE t_named_tuple_in;
