-- `JSONExtract` fills named tuples from JSON objects only (see the
-- `json_extract_named_tuples_as_objects` setting); unnamed tuples keep the
-- historical positional fill from arrays.

-- Named tuple from an array: defaults with the setting on (never an error),
-- historical positional fill with it off.
SET json_extract_named_tuples_as_objects = 1;
SELECT JSONExtract('{"t":["a","b"]}', 't', 'Tuple(x String, y String)');
SET json_extract_named_tuples_as_objects = 0;
SELECT JSONExtract('{"t":["a","b"]}', 't', 'Tuple(x String, y String)');
SET json_extract_named_tuples_as_objects = 1;

-- Objects fill named tuples by key under both values, order-independent.
SELECT JSONExtract('{"t":{"y":"b","x":"a"}}', 't', 'Tuple(x String, y String)');

-- Unnamed tuples: positional from arrays, unchanged by the setting.
SELECT JSONExtract('[3,5,7]', 'Tuple(Int64, Int64, Int64)');
SET json_extract_named_tuples_as_objects = 0;
SELECT JSONExtract('[3,5,7]', 'Tuple(Int64, Int64, Int64)');
SET json_extract_named_tuples_as_objects = 1;

-- Nested: outer object fill works, inner named tuple meeting an array yields
-- defaults inside a complex value (no error).
SELECT JSONExtract('{"o":{"inner":["a","b"],"k":7}}', 'o', 'Tuple(inner Tuple(p String, q String), k Int64)');

-- Named and unnamed side by side on the same array value.
SELECT JSONExtract('{"v":[1,2]}', 'v', 'Tuple(a Int64, b Int64)'), JSONExtract('{"v":[1,2]}', 'v', 'Tuple(Int64, Int64)');

-- Array of named tuples from array-of-arrays. The existing rule for invalid
-- elements applies unchanged: the array is empty only when every element fails,
-- while one valid element keeps its length and defaults the rest. With the
-- setting off every element is valid and fills positionally.
SELECT JSONExtract('{"l":[["a","b"],["c","d"]]}', 'l', 'Array(Tuple(x String, y String))');
SELECT JSONExtract('{"l":[{"x":"a","y":"b"},["c","d"]]}', 'l', 'Array(Tuple(x String, y String))');
SET json_extract_named_tuples_as_objects = 0;
SELECT JSONExtract('{"l":[["a","b"],["c","d"]]}', 'l', 'Array(Tuple(x String, y String))');
SET json_extract_named_tuples_as_objects = 1;

-- The same array reached through a named element: the failure stays inside the
-- element, and the sibling keeps its value.
SELECT JSONExtract('{"o":{"items":[["a","b"],["c","d"]],"k":7}}', 'o', 'Tuple(items Array(Tuple(x String, y String)), k Int64)');
SELECT JSONExtract('{"o":{"items":[{"x":"a","y":"b"},["c","d"]],"k":7}}', 'o', 'Tuple(items Array(Tuple(x String, y String)), k Int64)');
SET json_extract_named_tuples_as_objects = 0;
SELECT JSONExtract('{"o":{"items":[["a","b"],["c","d"]],"k":7}}', 'o', 'Tuple(items Array(Tuple(x String, y String)), k Int64)');
SET json_extract_named_tuples_as_objects = 1;

-- Scalar/tuple equivalence: the tuple element equals the scalar extraction for
-- object, array, and absent shapes.
SELECT
    tupleElement(JSONExtract(j, 'k', 'Tuple(a String)'), 'a') = JSONExtract(j, 'k', 'a', 'String')
FROM (SELECT arrayJoin(['{"k":{"a":"v"}}', '{"k":["v"]}', '{"k":7}', '{}']) AS j);

-- Duplicate keys (documented divergence, pinned): named tuple keeps the first
-- valid occurrence; the scalar path keeps the first occurrence.
SELECT JSONExtract('{"t":{"x":1,"x":2}}', 't', 'Tuple(x Int64)'), JSONExtract('{"t":{"x":1,"x":2}}', 't', 'x', 'Int64');

-- Same core cases under the RapidJSON parser.
SET allow_simdjson = 0;
SELECT JSONExtract('{"t":["a","b"]}', 't', 'Tuple(x String, y String)');
SET json_extract_named_tuples_as_objects = 0;
SELECT JSONExtract('{"t":["a","b"]}', 't', 'Tuple(x String, y String)');
SET json_extract_named_tuples_as_objects = 1;
SELECT JSONExtract('[3,5,7]', 'Tuple(Int64, Int64, Int64)');
SET allow_simdjson = 1;

-- Nullable named tuple from an array yields NULL at the top level (the failure
-- propagates through the Nullable wrapper, unlike the plain tuple's defaults).
SELECT JSONExtract('{"t":["a","b"]}', 't', 'Nullable(Tuple(x String, y String))');

-- Map values and JSONExtractKeysAndValues with named tuple values: arrays are
-- invalid values, so entries default or are dropped like any other mismatch.
SELECT JSONExtract('{"m":{"k1":["a","b"],"k2":{"x":"c","y":"d"}}}', 'm', 'Map(String, Tuple(x String, y String))');
SELECT JSONExtractKeysAndValues('{"k1":["a","b"],"k2":{"x":"c","y":"d"}}', 'Tuple(x String, y String)');

-- Typed paths of the JSON data type are exempt from the setting: they keep
-- the positional array fill regardless of its value.
CREATE TABLE t_05045_json (j JSON(t Tuple(x Int64, y Int64))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05045_json VALUES ('{"t":[1,2]}');
INSERT INTO t_05045_json VALUES ('{"t":{"x":3,"y":4}}');
SET json_extract_named_tuples_as_objects = 0;
INSERT INTO t_05045_json VALUES ('{"t":[5,6]}');
SET json_extract_named_tuples_as_objects = 1;
SELECT j.t FROM t_05045_json ORDER BY j.t.x;
DROP TABLE t_05045_json;
