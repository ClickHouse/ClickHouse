-- Test that mapContainsValue() is rewritten to the Map values subcolumn.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_map_contains_value_subcolumn;

CREATE TABLE t_map_contains_value_subcolumn
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_map_contains_value_subcolumn VALUES
    (0, {'a': 1, 'b': 2}),
    (1, {'a': 3, 'b': 4}),
    (2, {'a': 5, 'b': 6});

SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_map_contains_value_subcolumn WHERE mapContainsValue(m, 4))
WHERE explain LIKE '%m.values%';

SELECT count() = 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_map_contains_value_subcolumn WHERE mapContainsValue(m, 4))
WHERE explain LIKE '%mapContainsValue%';

SELECT id
FROM t_map_contains_value_subcolumn
WHERE mapContainsValue(m, 4)
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT id
FROM t_map_contains_value_subcolumn
WHERE mapContainsValue(m, 4)
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT count()
FROM t_map_contains_value_subcolumn
WHERE mapContainsValue(m, 99)
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT count()
FROM t_map_contains_value_subcolumn
WHERE mapContainsValue(m, 99)
SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_map_contains_value_subcolumn;

-- Keep the Map adapter's LowCardinality/FixedString coercion semantics.
DROP TABLE IF EXISTS t_map_contains_value_low_cardinality;

CREATE TABLE t_map_contains_value_low_cardinality
(
    id UInt64,
    m Map(String, LowCardinality(FixedString(3)))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_map_contains_value_low_cardinality VALUES
    (0, {'a': 'V0'}),
    (1, {'a': 'V1A'}),
    (2, {'a': 'XYZ'});

SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_map_contains_value_low_cardinality WHERE mapContainsValue(m, 'V0'))
WHERE explain LIKE '%m.values%';

SELECT id
FROM t_map_contains_value_low_cardinality
WHERE mapContainsValue(m, 'V0')
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT id
FROM t_map_contains_value_low_cardinality
WHERE mapContainsValue(m, 'V0')
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT count()
FROM t_map_contains_value_low_cardinality
WHERE mapContainsValue(m, toFixedString('V0', 5))
SETTINGS optimize_functions_to_subcolumns = 1; -- { serverError TOO_LARGE_STRING_SIZE }

SELECT count()
FROM t_map_contains_value_low_cardinality
WHERE mapContainsValue(m, toFixedString('V0', 5))
SETTINGS optimize_functions_to_subcolumns = 0; -- { serverError TOO_LARGE_STRING_SIZE }

DROP TABLE t_map_contains_value_low_cardinality;
