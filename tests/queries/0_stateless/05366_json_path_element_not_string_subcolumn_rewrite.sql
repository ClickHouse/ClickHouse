DROP TABLE IF EXISTS t_json_path_element;
CREATE TABLE t_json_path_element (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_path_element VALUES ('{"x": {"y": [{"z": 1, "o": {"w": 1}}, {"z": 2, "o": {"w": 2}}]}, "a": {"b": {"c": 3}}}');

-- A path element `tupleElement` or `arrayElement` rejects must not be accepted by the subcolumn rewrite either.
SET dynamic_throw_on_type_mismatch = 0;
SELECT
    tupleElement(json.x.y[2], toNullable('z')),
    tupleElement(json.x.y[2], toLowCardinality('z')),
    tupleElement(json.x.y[2], toFixedString('z', 1)),
    tupleElement(tupleElement(json.x.y[2], toNullable('o')), 'w'),
    json['a'][toFixedString('b', 1)],
    json['a'][toFixedString('b', 1)]['c'],
    json['a'][toNullable('b')]['c'],
    json['a'][toLowCardinality('b')]['c'],
    tupleElement(tupleElement(json.x.y[2], 'o'), 'w')
FROM t_json_path_element
SETTINGS optimize_functions_to_subcolumns = 1;
SELECT
    tupleElement(json.x.y[2], toNullable('z')),
    tupleElement(json.x.y[2], toLowCardinality('z')),
    tupleElement(json.x.y[2], toFixedString('z', 1)),
    tupleElement(tupleElement(json.x.y[2], toNullable('o')), 'w'),
    json['a'][toFixedString('b', 1)],
    json['a'][toFixedString('b', 1)]['c'],
    json['a'][toNullable('b')]['c'],
    json['a'][toLowCardinality('b')]['c'],
    tupleElement(tupleElement(json.x.y[2], 'o'), 'w')
FROM t_json_path_element
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT tupleElement(json.x.y[2], toNullable('z')) FROM t_json_path_element
SETTINGS optimize_functions_to_subcolumns = 1, dynamic_throw_on_type_mismatch = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The rewrite still applies to the elements the functions accept.
SET optimize_functions_to_subcolumns = 1;
SELECT countIf(explain ILIKE '%json.x.y.:`Array(JSON)`.o.w%') > 0, countIf(explain ILIKE '%json.@`a`.b.c%') > 0
FROM (EXPLAIN QUERY TREE run_passes = 1
      SELECT tupleElement(tupleElement(json.x.y[2], 'o'), 'w'), json['a'][toNullable('b')]['c'] FROM t_json_path_element);
SELECT countIf(explain ILIKE '%json.@`a`.b.c%') > 0
FROM (EXPLAIN QUERY TREE run_passes = 1
      SELECT json['a'][toLowCardinality('b')]['c'] FROM t_json_path_element);

DROP TABLE t_json_path_element;
