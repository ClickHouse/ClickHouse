-- `max_dynamic_subcolumns_in_json_type_parsing` must also cap objects nested inside a dynamic path,
-- whose columns are built from the type inferred during parsing.
-- One table per insert: a path whose nested type differs between parts reads as shared data anyway.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_json_parsing_cap_nested;
DROP TABLE IF EXISTS t_json_parsing_cap_nested_deep;

CREATE TABLE t_json_parsing_cap_nested (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_parsing_cap_nested
SELECT '{"a" : [{"a" : 42, "b" : 42, "c" : 42}]}'
SETTINGS max_dynamic_subcolumns_in_json_type_parsing = 1;

SELECT JSONDynamicPaths(json), JSONSharedDataPaths(json) FROM t_json_parsing_cap_nested;
SELECT JSONDynamicPaths(json.a[][1]), JSONSharedDataPaths(json.a[][1]) FROM t_json_parsing_cap_nested;

CREATE TABLE t_json_parsing_cap_nested_deep (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_parsing_cap_nested_deep
SELECT '{"a" : [{"b" : [{"c" : 42, "d" : 42, "e" : 42}]}]}'
SETTINGS max_dynamic_subcolumns_in_json_type_parsing = 1;

SELECT JSONDynamicPaths(level2), JSONSharedDataPaths(level2)
FROM (SELECT level1.b[][1] AS level2 FROM (SELECT json.a[][1] AS level1 FROM t_json_parsing_cap_nested_deep));

DROP TABLE t_json_parsing_cap_nested;
DROP TABLE t_json_parsing_cap_nested_deep;
