-- Regression test for the implicit comparison casts of named tuples (the left hand side of IN
-- is cast to the type of the set elements with an accurate cast). With
-- enable_named_columns_in_function_tuple enabled by default, such casts must not silently map
-- the elements by name and fill the absent ones with default values: the by-name mapping is used
-- only when both tuples have exactly the same set of names, otherwise the conversion is positional.

DROP TABLE IF EXISTS t_named_tuple_in;

CREATE TABLE t_named_tuple_in (id UInt64, str String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_named_tuple_in SELECT number, concat('Hello', toString(number)) FROM numbers(100);

-- Disjoint names and different arity: must be an error, not a match of default values
-- (this used to match all 100 rows by comparing default-filled values).
SELECT count() FROM t_named_tuple_in WHERE (id, str) IN (SELECT tuple(number) FROM numbers(100)); -- { serverError TYPE_MISMATCH, NUMBER_OF_COLUMNS_DOESNT_MATCH, ILLEGAL_TYPE_OF_ARGUMENT }

-- Disjoint names, same arity: positional conversion, as for unnamed tuples.
SELECT count() FROM t_named_tuple_in WHERE (id, str) IN (
    SELECT tuple(x, y) FROM (SELECT number AS x, concat('Hello', toString(number)) AS y FROM numbers(10) WHERE number = 5));

-- Same set of names in a different order: elements are matched by name
-- (a positional conversion would fail here on UInt64 vs String).
SELECT count() FROM t_named_tuple_in WHERE (id, str) IN (
    SELECT tuple(str, id) FROM (SELECT number AS id, concat('Hello', toString(number)) AS str FROM numbers(10) WHERE number = 7));

-- Explicit CAST keeps the permissive named tuple semantics: extra source fields are dropped
-- and missing destination fields are filled with default values.
SELECT (1 AS a, 2 AS b)::Tuple(a UInt32, c UInt32);

DROP TABLE t_named_tuple_in;
