-- Validation and edge cases for the GeoJSON output format.

-- The column named `id` becomes the Feature id, which GeoJSON requires to be a string or number, so a
-- column of any other type (such as an array) is rejected.
SELECT [1, 2] AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON; -- { clientError BAD_ARGUMENTS }

-- A Bool id is rejected: although stored as a numeric type, it serializes as a JSON boolean, which is
-- not a valid Feature id.
SELECT false AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON; -- { clientError BAD_ARGUMENTS }

-- Only one column can map to the Feature id; a second column named `id` is rejected rather than
-- silently dropped.
SELECT id, id, (1.0, 2.0)::Point AS geometry FROM (SELECT 1 AS id) FORMAT GeoJSON; -- { clientError BAD_ARGUMENTS }

-- A null id is omitted from the Feature, including when it arrives through a `LowCardinality(Nullable)`
-- column rather than a plain `Nullable` column.
SELECT CAST(NULL AS LowCardinality(Nullable(String))) AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;

-- A literal `NULL AS id` has type `Nullable(Nothing)`; it is accepted and the id is omitted.
SELECT NULL AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;

-- A literal `NULL AS properties` has type `Nullable(Nothing)`; it is written as the top-level
-- `"properties": null` rather than nested inside a `{"properties": null}` object.
SELECT (1.0, 2.0)::Point AS geometry, NULL AS properties FORMAT GeoJSON;

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

-- A numeric Feature id is always a bare JSON number, even when `output_format_json_quote_64bit_integers`
-- or `output_format_json_quote_decimals` is enabled (those settings still apply to property values).
SELECT 18446744073709551615::UInt64 AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON SETTINGS output_format_json_quote_64bit_integers = 1;
SELECT 1.23::Decimal64(2) AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON SETTINGS output_format_json_quote_decimals = 1;

-- The object forcing applies only to a lone `properties` object column; an ordinary property column
-- named otherwise follows the user's JSON settings, so a named Tuple or Map is written as an array when
-- the corresponding setting requests it.
SELECT (1.0, 2.0)::Point AS geometry, (1, 'x')::Tuple(a UInt8, b String) AS t FORMAT GeoJSON SETTINGS output_format_json_named_tuples_as_objects = 0;
SELECT (1.0, 2.0)::Point AS geometry, map('a', 'b') AS m FORMAT GeoJSON SETTINGS output_format_json_map_as_array_of_tuples = 1;
