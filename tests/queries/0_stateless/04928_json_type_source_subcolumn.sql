-- The `__source` subcolumn keeps the JSON text of the row.

SELECT toTypeName('{"a" : 42}'::JSON(with_source=1).__source);

-- Original text is preserved as is.
SELECT ('{"a" : 42,   "b" : {"c" : "Hello"}}'::JSON(with_source=1)).__source;
SELECT getSubcolumn('{"a" : 42}'::JSON(with_source=1), '__source');

-- Without the parameter it's an ordinary path.
SELECT toTypeName('{"a" : 42}'::JSON.__source);

DROP TABLE IF EXISTS t_json_source_subcolumn;
CREATE TABLE t_json_source_subcolumn (id UInt64, json JSON(with_source=1, a UInt32)) ENGINE = Memory;
INSERT INTO t_json_source_subcolumn VALUES (1, '{"a" : 42, "b" : "Hello"}'), (2, '{}'), (3, '{"a" : 43, "c" : [1, 2, 3]}');
SELECT id, json.__source FROM t_json_source_subcolumn ORDER BY id;
SELECT id, json, json.a FROM t_json_source_subcolumn ORDER BY id;

-- The source is not a path of the object.
SELECT DISTINCT arrayJoin(JSONAllPaths(json)) AS path FROM t_json_source_subcolumn ORDER BY path;
SELECT DISTINCT arrayJoin(JSONDynamicPaths(json)) AS path FROM t_json_source_subcolumn ORDER BY path;
DROP TABLE t_json_source_subcolumn;

-- Rows created without the original text get it from the object itself.
SELECT json.__source FROM (SELECT materialize('{"a" :  42}')::JSON(with_source=1) AS json FROM numbers(2)) GROUP BY json;
SELECT (materialize('{}')::JSON(with_source=1)).__source;

-- Values from other formats.
SELECT json.__source FROM format(TSV, 'json JSON(with_source=1)', '{"a" : 42, "b" : 1}');
SELECT json.__source FROM format(CSV, 'json JSON(with_source=1)', '"{""a"" : 42}"');
