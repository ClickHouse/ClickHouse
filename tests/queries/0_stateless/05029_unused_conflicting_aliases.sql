-- Multiple expressions with the same alias are an error only if the alias is actually used:
-- referenced by some identifier, or naming a projection column of the result.
-- Aliases that only give names to nested expressions (e.g. to the elements of named tuples) can repeat.
-- https://github.com/ClickHouse/ClickHouse/issues/89201

-- The relaxation is implemented only in the analyzer; the old analyzer still rejects
-- every repeated alias with `MULTIPLE_EXPRESSIONS_FOR_ALIAS`.
SET enable_analyzer = 1;

SELECT 'Aliases of tuple elements can repeat in different tuples';
SELECT tuple(1 AS x, 2 AS y, 3 AS z) AS t1, tuple(4 AS x, 5 AS y, 6 AS z) AS t2, toTypeName(t1), toTypeName(t2) SETTINGS enable_named_columns_in_function_tuple = 1;
SELECT tuple(1 AS x, 2 AS y), tuple(3 AS x, 4 AS y);
SELECT tuple(number + 1 AS x) AS t1, tuple(number + 2 AS x) AS t2 FROM numbers(2) SETTINGS enable_named_columns_in_function_tuple = 1;

SELECT 'Colliding column names of conflicting constant expressions are disambiguated';
DESCRIBE (SELECT tuple(1 AS x), tuple(2 AS x));

SELECT 'The same argument named differently in different tuples produces different tuples';
SELECT tuple(a AS x) AS t1, tuple(a AS y) AS t2, toTypeName(t1), toTypeName(t2) FROM (SELECT 1 AS a) SETTINGS enable_named_columns_in_function_tuple = 1;

SELECT 'A tuple element name can shadow the name of the source column';
-- Resolving the inner `a` of `a AS a` probes the alias `a`, hits the cycle guard, and falls back
-- to the source column. The probe must not mark the alias as used.
SELECT tuple(a AS a), tuple(b AS a) FROM (SELECT 1 AS a, 2 AS b) SETTINGS enable_named_columns_in_function_tuple = 1;
DESCRIBE (SELECT tuple(a AS a), tuple(b AS a) FROM (SELECT 1 AS a, 2 AS b)) SETTINGS enable_named_columns_in_function_tuple = 1;

SELECT 'Unused conflicting aliases of nested expressions are allowed';
WITH 1 AS x SELECT tuple(2 AS x) SETTINGS enable_named_columns_in_function_tuple = 1;
WITH 1 AS x, 2 AS x SELECT 3;
SELECT arrayMap(x -> (x + 1) AS y, [3, 5]), arrayMap(x -> (x || 'hello') AS y, ['qq', 'ww']);

SELECT 'An alias defined inside a tuple can be referenced if it is defined once';
SELECT tuple(1 AS x, 2 AS y), x + 10, y * 10 SETTINGS enable_named_columns_in_function_tuple = 1;

SELECT 'Referencing an alias that has conflicting definitions is an error';
SELECT tuple(1 AS x), tuple(2 AS x), x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH 1 AS x SELECT tuple(2 AS x), x SETTINGS enable_named_columns_in_function_tuple = 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT 1 AS x, 2 AS x, x + 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT 1 AS x, 2 AS x ORDER BY x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH x -> x + 1 AS lambda, x -> x + 2 AS lambda SELECT lambda(1); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT 'Conflicting aliases that name projection columns are an error even if not referenced';
SELECT 1 AS x, 2 AS x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT tuple(1 AS x), 2 AS x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT 'Identical duplicate definitions of a referenced alias are still allowed';
SELECT 1 AS x, 1 AS x, x + 1;

SELECT 'Default expressions of different columns can repeat an alias of a nested expression';
DROP TABLE IF EXISTS t_repeated_alias_in_defaults;
CREATE TABLE t_repeated_alias_in_defaults
(
    str String,
    materialized_a String MATERIALIZED concat(str, 'a' AS a)
)
ENGINE = MergeTree ORDER BY tuple();

ALTER TABLE t_repeated_alias_in_defaults ADD COLUMN materialized_b String MATERIALIZED concat(str, 'b' AS a);
ALTER TABLE t_repeated_alias_in_defaults ADD COLUMN default_c String DEFAULT concat(str, 'c' AS a);
INSERT INTO t_repeated_alias_in_defaults(str) VALUES ('x');
SELECT materialized_a, materialized_b, default_c FROM t_repeated_alias_in_defaults;
DROP TABLE t_repeated_alias_in_defaults;
