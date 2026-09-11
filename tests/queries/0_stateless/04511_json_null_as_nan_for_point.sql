-- A `null` in a Float element of a composite type must decode to NaN, not to the element default.
-- ClickHouse writes NaN as `null` in JSON output, so decoding it back as 0 broke the round trip and
-- made `Point` (= Tuple(Float64, Float64)) read [11,null] as (11,0) while TSV parses NaN correctly.
-- Covers every format listed in issue #111917.

SET allow_suspicious_low_cardinality_types = 1;

SELECT 'JSON';
SELECT * FROM format(JSON, 'a Point', '{"meta":[{"name":"a","type":"Point"}],"data":[{"a":[11,null]}]}');

SELECT 'JSONColumns';
SELECT * FROM format(JSONColumns, 'a Point', '{"a":[[11,null]]}');

SELECT 'JSONColumnsWithMetadata';
SELECT * FROM format(JSONColumnsWithMetadata, 'a Point', '{"meta":[{"name":"a","type":"Point"}],"data":{"a":[[11,null]]}}');

SELECT 'JSONCompact';
SELECT * FROM format(JSONCompact, 'a Point', '{"meta":[{"name":"a","type":"Point"}],"data":[[[11,null]]]}');

SELECT 'JSONCompactColumns';
SELECT * FROM format(JSONCompactColumns, 'a Point', '[[[11,null]]]');

SELECT 'JSONEachRow';
SELECT * FROM format(JSONEachRow, 'a Point', '{"a":[11,null]}');
SELECT * FROM format(JSONEachRow, 'a Point', '{"a":[null,null]}');

SELECT 'JSONCompactEachRow';
SELECT * FROM format(JSONCompactEachRow, 'a Point', '[[11,null]]');

SELECT 'JSONCompactEachRowWithNames';
SELECT * FROM format(JSONCompactEachRowWithNames, 'a Point', '["a"]\n[[11,null]]');

SELECT 'JSONCompactEachRowWithNamesAndTypes';
SELECT * FROM format(JSONCompactEachRowWithNamesAndTypes, 'a Point', '["a"]\n["Point"]\n[[11,null]]');

SELECT 'JSONObjectEachRow';
SELECT * FROM format(JSONObjectEachRow, 'a Point', '{"r1":{"a":[11,null]}}');

SELECT 'other geo types built on Point';
SELECT * FROM format(JSONEachRow, 'a Ring', '{"a":[[11,null],[null,12]]}');
SELECT * FROM format(JSONEachRow, 'a LineString', '{"a":[[11,null]]}');
SELECT * FROM format(JSONEachRow, 'a Polygon', '{"a":[[[11,null],[null,12]]]}');

SELECT 'float elements of tuple, array and map';
SELECT * FROM format(JSONEachRow, 'a Tuple(Float64, Float64)', '{"a":[11,null]}');
SELECT * FROM format(JSONEachRow, 'a Array(Float64)', '{"a":[11,null,13]}');
SELECT * FROM format(JSONEachRow, 'a Array(Float32)', '{"a":[11,null,13]}');
SELECT * FROM format(JSONEachRow, 'a Array(LowCardinality(Float64))', '{"a":[11,null,13]}');
SELECT * FROM format(JSONEachRow, 'a Map(String, Float64)', '{"a":{"x":null}}');

SELECT 'non-float elements still take the element default';
SELECT * FROM format(JSONEachRow, 'a Tuple(UInt64, UInt64)', '{"a":[11,null]}');
SELECT * FROM format(JSONEachRow, 'a Array(Int32)', '{"a":[11,null]}');
SELECT * FROM format(JSONEachRow, 'a Tuple(Float64, String)', '{"a":[11,null]}') SETTINGS input_format_null_as_default = 1;

-- A whole column is not a composite element: it may carry a DEFAULT expression that
-- input_format_null_as_default is meant to trigger, so a top-level null keeps using the default.
SELECT 'a top-level null still uses the column default';
SELECT * FROM format(JSONEachRow, 'a Float64', '{"a":null}') SETTINGS input_format_null_as_default = 1;
SELECT * FROM format(JSONEachRow, 'a Float32', '{"a":null}') SETTINGS input_format_null_as_default = 1;

SELECT 'nullable is untouched';
SELECT * FROM format(JSONEachRow, 'a Tuple(Float64, Nullable(Float64))', '{"a":[11,null]}');
SELECT * FROM format(JSONEachRow, 'a Nullable(Float64)', '{"a":null}');

SELECT 'round trip';
SELECT CAST((11, nan), 'Point') AS a FORMAT JSONEachRow;

SELECT 'null_as_default = 0 was already correct';
SELECT * FROM format(JSONEachRow, 'a Point', '{"a":[11,null]}') SETTINGS input_format_null_as_default = 0;
