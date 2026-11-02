SET enable_nullable_tuple_type = 1;
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET join_use_nulls = 0;

DROP TABLE IF EXISTS nullable_tuple_elements;
CREATE TABLE nullable_tuple_elements
(
    id UInt64,
    t Nullable(Tuple(u UInt64, n Nullable(String), lc LowCardinality(String),
        lcn LowCardinality(Nullable(String)), v Variant(UInt64, String), d Dynamic))
) ENGINE = Memory;

INSERT INTO nullable_tuple_elements
SELECT number, if(number = 1, NULL, tuple(number + 10, if(number = 2, NULL, 's'),
    toLowCardinality('lc'), toLowCardinality(if(number = 2, NULL, 'lcn')),
    (number + 50)::Variant(UInt64, String), (number + 60)::Dynamic))
FROM numbers(3);

-- Extracted elements retain both parent and element NULLs, including dictionary and variant columns.
SELECT id, tupleElement(t, 'u'), tupleElement(t, 'n'), tupleElement(t, 'lc'),
    tupleElement(t, 'lcn'), tupleElement(t, 'v'), tupleElement(t, 'd')
FROM nullable_tuple_elements ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, tupleElement(t, 'u'), tupleElement(t, 'n'), tupleElement(t, 'lc'),
    tupleElement(t, 'lcn'), tupleElement(t, 'v'), tupleElement(t, 'd')
FROM nullable_tuple_elements ORDER BY id;
SELECT toTypeName(tupleElement(t, 'u')), toTypeName(tupleElement(t, 'n')),
    toTypeName(tupleElement(t, 'lc')), toTypeName(tupleElement(t, 'lcn')),
    toTypeName(tupleElement(t, 'v')), toTypeName(tupleElement(t, 'd')) FROM nullable_tuple_elements LIMIT 1;

SELECT count() FROM (EXPLAIN QUERY TREE
    SELECT tupleElement(t, 'u'), tupleElement(t, 'n'), tupleElement(t, 'lc'),
        tupleElement(t, 'lcn'), tupleElement(t, 'v'), tupleElement(t, 'd')
    FROM nullable_tuple_elements)
WHERE explain LIKE '%column_name: t.%';

-- Named, unsigned, signed positive and negative indices identify the same element.
SELECT tupleElement(t, 'u'), tupleElement(t, 1), tupleElement(t, 1::Int64), tupleElement(t, -6)
FROM nullable_tuple_elements ORDER BY id;
SELECT count() FROM (EXPLAIN QUERY TREE
    SELECT tupleElement(t, 1), tupleElement(t, 1::Int64), tupleElement(t, -6) FROM nullable_tuple_elements)
WHERE explain LIKE '%column_name: t.u%';
SELECT tupleElement(t, 'absent', 99) FROM nullable_tuple_elements ORDER BY id;
SELECT count() FROM (SELECT tupleElement(t, 'u') FROM nullable_tuple_elements WHERE id > 10);

-- A filter can read an element while the projection reads the entire tuple.
SELECT id, t FROM nullable_tuple_elements WHERE tupleElement(t, 'u') = 12;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT t FROM nullable_tuple_elements WHERE tupleElement(t, 'u') = 12)
WHERE explain LIKE '%column_name: t.u%';

-- Elements on the unmatched side of an outer join remain NULL.
SELECT l.id, tupleElement(r.t, 'u') FROM nullable_tuple_elements AS l LEFT JOIN nullable_tuple_elements AS r
ON l.id = r.id + 1 ORDER BY l.id SETTINGS join_use_nulls = 0;
SELECT l.id, tupleElement(r.t, 'u') FROM nullable_tuple_elements AS l LEFT JOIN nullable_tuple_elements AS r
ON l.id = r.id + 1 ORDER BY l.id SETTINGS join_use_nulls = 1;

-- Element reads use the same nullable representation in wide and compact parts.
DROP TABLE IF EXISTS nullable_tuple_elements_wide;
DROP TABLE IF EXISTS nullable_tuple_elements_compact;
CREATE TABLE nullable_tuple_elements_wide AS nullable_tuple_elements ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
CREATE TABLE nullable_tuple_elements_compact AS nullable_tuple_elements ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000;
INSERT INTO nullable_tuple_elements_wide SELECT * FROM nullable_tuple_elements;
INSERT INTO nullable_tuple_elements_compact SELECT * FROM nullable_tuple_elements;
SELECT id, tupleElement(t, 'u'), tupleElement(t, 'n'), tupleElement(t, 'lc'),
    tupleElement(t, 'lcn'), tupleElement(t, 'v'), tupleElement(t, 'd')
FROM nullable_tuple_elements_wide ORDER BY id;
SELECT id, tupleElement(t, 'u'), tupleElement(t, 'n'), tupleElement(t, 'lc'),
    tupleElement(t, 'lcn'), tupleElement(t, 'v'), tupleElement(t, 'd')
FROM nullable_tuple_elements_compact ORDER BY id SETTINGS max_block_size = 1;
DROP TABLE nullable_tuple_elements_wide;
DROP TABLE nullable_tuple_elements_compact;

DROP TABLE nullable_tuple_elements;

DROP TABLE IF EXISTS nullable_tuple_containers;
CREATE TABLE nullable_tuple_containers
(
    id UInt64,
    t Nullable(Tuple(a Array(UInt64), m Map(String, UInt64), inner Tuple(x UInt64)))
) ENGINE = Memory;

-- `nullIf` preserves non-default nested values beneath the parent null map.
INSERT INTO nullable_tuple_containers
SELECT number, nullIf(tuple([number + 20], map('k', number + 30), tuple(number + 40)),
    tuple([21::UInt64], map('k', 31::UInt64), tuple(41::UInt64))) FROM numbers(3);
SELECT id, tupleElement(t, 'a'), tupleElement(t, 'm') FROM nullable_tuple_containers ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, tupleElement(t, 'a'), tupleElement(t, 'm') FROM nullable_tuple_containers ORDER BY id;
SELECT count() FROM (EXPLAIN QUERY TREE
    SELECT tupleElement(t, 'a'), tupleElement(t, 'm') FROM nullable_tuple_containers)
WHERE explain LIKE '%column_name: t.%';

-- Nested tuple extraction follows the configured nullability of extracted tuple subcolumns.
SELECT
    (SELECT groupArray(tuple(id, tupleElement(t, 'inner'), tupleElement(tupleElement(t, 'inner'), 'x')))
     FROM nullable_tuple_containers SETTINGS optimize_functions_to_subcolumns = 0)
    =
    (SELECT groupArray(tuple(id, tupleElement(t, 'inner'), tupleElement(tupleElement(t, 'inner'), 'x')))
     FROM nullable_tuple_containers SETTINGS optimize_functions_to_subcolumns = 1);
SELECT
    (SELECT count() FROM (EXPLAIN QUERY TREE SELECT tupleElement(t, 'inner') FROM nullable_tuple_containers)
     WHERE explain LIKE '%column_name: t.inner%')
    = startsWith(toTypeName(tupleElement(NULL::Nullable(Tuple(inner Tuple(x UInt64))), 'inner')), 'Nullable');
DROP TABLE nullable_tuple_containers;

-- A nullable JSON argument also accepts `tupleElement` and keeps its existing extraction semantics.
DROP TABLE IF EXISTS nullable_tuple_json;
CREATE TABLE nullable_tuple_json (id UInt64, j Nullable(JSON(s String))) ENGINE = Memory;
INSERT INTO nullable_tuple_json VALUES (0, '{"s":"value"}'), (1, NULL);
SELECT id, tupleElement(j, 's') FROM nullable_tuple_json ORDER BY id;
DROP TABLE nullable_tuple_json;

-- A physical column or a dotted sibling must not capture the rewritten element name.
DROP TABLE IF EXISTS nullable_tuple_collision;
CREATE TABLE nullable_tuple_collision
(
    t Nullable(Tuple(x UInt64, a Tuple(b UInt64), `a.b` UInt64)),
    `t.x` Nullable(UInt64)
) ENGINE = Memory;
INSERT INTO nullable_tuple_collision VALUES ((10, (20), 30), 99), (NULL, 88);
SELECT tupleElement(t, 'x'), tupleElement(t, 'a.b') FROM nullable_tuple_collision;
SELECT count() FROM (EXPLAIN QUERY TREE
    SELECT tupleElement(t, 'x'), tupleElement(t, 'a.b') FROM nullable_tuple_collision)
WHERE explain LIKE '%column_name: t.%';
DROP TABLE nullable_tuple_collision;

-- A stored nested tuple can expose a different element type because of its nullable ancestor.
DROP TABLE IF EXISTS nullable_tuple_nested_type;
CREATE TABLE nullable_tuple_nested_type (t Nullable(Tuple(inner Tuple(x UInt64)))) ENGINE = Memory;
INSERT INTO nullable_tuple_nested_type VALUES (((42))), (NULL);
SELECT
    (SELECT groupArray(tuple(tupleElement(t.inner, 'x')))
     FROM nullable_tuple_nested_type SETTINGS optimize_functions_to_subcolumns = 0)
    =
    (SELECT groupArray(tuple(tupleElement(t.inner, 'x')))
     FROM nullable_tuple_nested_type SETTINGS optimize_functions_to_subcolumns = 1);
DROP TABLE nullable_tuple_nested_type;

-- Predicates on a tuple element used in the primary key retain the indexed expression.
DROP TABLE IF EXISTS nullable_tuple_key;
CREATE TABLE nullable_tuple_key (id UInt64, t Nullable(Tuple(u UInt64)))
ENGINE = MergeTree ORDER BY tupleElement(t, 'u') SETTINGS allow_nullable_key = 1;
INSERT INTO nullable_tuple_key VALUES (0, (10)), (1, NULL), (2, (12));
SELECT id FROM nullable_tuple_key WHERE tupleElement(t, 'u') = 12;
SELECT count() FROM (EXPLAIN QUERY TREE
    SELECT id FROM nullable_tuple_key WHERE tupleElement(t, 'u') = 12)
WHERE explain LIKE '%column_name: t.u%';
DROP TABLE nullable_tuple_key;
