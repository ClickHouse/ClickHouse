-- Validation and edge cases for the GeoJSON output format.

-- The column named `id` becomes the Feature id, which GeoJSON requires to be a string or number, so a
-- column of any other type (such as an array) is rejected.
SELECT [1, 2] AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON; -- { clientError BAD_ARGUMENTS }

-- A null id is omitted from the Feature, including when it arrives through a `LowCardinality(Nullable)`
-- column rather than a plain `Nullable` column.
SELECT CAST(NULL AS LowCardinality(Nullable(String))) AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;

-- A named Tuple used as the lone `properties` column is always written as an object, even when
-- `output_format_json_named_tuples_as_objects` is disabled.
SELECT (1.0, 2.0)::Point AS geometry, (1, 'x')::Tuple(num UInt8, str String) AS properties FORMAT GeoJSON SETTINGS output_format_json_named_tuples_as_objects = 0;

-- A Map used as the lone `properties` column is always written as an object, even when
-- `output_format_json_map_as_array_of_tuples` is enabled.
SELECT (1.0, 2.0)::Point AS geometry, map('a', 'b') AS properties FORMAT GeoJSON SETTINGS output_format_json_map_as_array_of_tuples = 1;

-- A geometry column wrapped in Nullable (possible under enable_nullable_tuple_type) is recognized as
-- the geometry, and a null value is written as a null geometry.
SELECT CAST((1.0, 2.0), 'Nullable(Point)') AS geometry FORMAT GeoJSON SETTINGS enable_nullable_tuple_type = 1;
SELECT CAST(NULL AS Nullable(Point)) AS geometry FORMAT GeoJSON SETTINGS enable_nullable_tuple_type = 1;

-- `output_format_json_quote_64bit_floats` applies to Float64 property values, but coordinates remain
-- bare JSON numbers regardless of that setting.
SELECT (1.0, 2.0)::Point AS geometry, toFloat64(1.5) AS p FORMAT GeoJSON SETTINGS output_format_json_quote_64bit_floats = 1;
