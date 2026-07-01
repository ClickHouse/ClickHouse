-- Tests for the GeoJSON input format.

-- Basic FeatureCollection with a Point geometry.
SELECT id, geometry, JSONExtractString(properties, 'name') AS name
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "1",
            "geometry": {"type": "Point", "coordinates": [13.4050, 52.5200]},
            "properties": {"name": "Berlin"}
        },
        {
            "type": "Feature",
            "id": "2",
            "geometry": {"type": "Point", "coordinates": [2.3522, 48.8566]},
            "properties": {"name": "Paris"}
        }
    ]
}');

-- Polygon geometry.
SELECT id, variantType(geometry) AS geo_type
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0,0],[1,0],[1,1],[0,1],[0,0]]]
            },
            "properties": {}
        }
    ]
}');

-- LineString geometry.
SELECT variantType(geometry) AS geo_type, geometry.LineString
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[0,0],[1,2],[3,4]]}, "properties": {}}
    ]
}');

-- MultiLineString geometry.
SELECT variantType(geometry) AS geo_type, geometry.MultiLineString
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "MultiLineString", "coordinates": [[[0,0],[1,1]],[[2,2],[3,3]]]}, "properties": {}}
    ]
}');

-- A missing id should produce NULL (`id` is Nullable(String)); a null geometry should produce the None variant.
SELECT id IS NULL, variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": null,
            "properties": {"key": "value"}
        }
    ]
}');

-- Key ordering: coordinates before type.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {"coordinates": [1.0, 2.0], "type": "Point"},
            "properties": {}
        }
    ]
}');

-- Schema inference returns the fixed schema.
DESCRIBE format('GeoJSON', '{"type":"FeatureCollection","features":[]}');

-- GeometryCollection: default behaviour is to throw.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "GeometryCollection",
                "geometries": [
                    {"type": "Point", "coordinates": [0, 0]}
                ]
            },
            "properties": {}
        }
    ]
}'); -- { serverError INCORRECT_DATA }

-- GeometryCollection: with null handling inserts NULL for geometry.
SELECT isNull(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "GeometryCollection",
                "geometries": [
                    {"type": "Point", "coordinates": [0, 0]}
                ]
            },
            "properties": {}
        }
    ]
}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null';

-- MultiPoint cannot be represented in the Geometry type: it throws by default instead of silently dropping data.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "MultiPoint", "coordinates": [[0, 0], [1, 1]]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- MultiPoint with null handling inserts NULL for geometry.
SELECT isNull(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "MultiPoint", "coordinates": [[0, 0], [1, 1]]}, "properties": {}}
    ]
}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null';

-- A duplicate fixed field within a feature is rejected as bad input.
SELECT id
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1", "id": "2", "geometry": null, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A 'geometry' column that is not of type Geometry is rejected instead of causing undefined behaviour.
SELECT *
FROM format('GeoJSON', 'id String, geometry String, properties JSON', '{"type":"FeatureCollection","features":[]}'); -- { serverError BAD_ARGUMENTS }

-- A 'geometry' column that is only a partial Geometry Variant is rejected (would otherwise lose data as NULL).
SELECT *
FROM format('GeoJSON', 'geometry Variant(Point), properties JSON', '{"type":"FeatureCollection","features":[]}'); -- { serverError BAD_ARGUMENTS }

-- An unsupported column is rejected (would otherwise produce inconsistently sized columns).
SELECT *
FROM format('GeoJSON', 'geometry Geometry, extra Int32', '{"type":"FeatureCollection","features":[]}'); -- { serverError BAD_ARGUMENTS }

-- An unknown or misspelled geometry type is malformed input and is always rejected, even with null handling.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Piont", "coordinates": [0, 0]}, "properties": {}}
    ]
}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }

-- A supported geometry type without a 'coordinates' member is malformed input.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Point"}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A missing comma between features is rejected instead of being silently accepted.
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":null,"properties":{}} {"type":"Feature","geometry":null,"properties":{}}]}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- 'Ring' is part of ClickHouse's Geometry type but is not a valid GeoJSON geometry type, so it is rejected.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Ring", "coordinates": [[0, 0], [1, 1]]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- 'geometry' is a required member of a feature: a feature without it is rejected.
SELECT id
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1", "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- 'properties' is a required member of a feature: a feature without it is rejected.
SELECT id
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1", "geometry": null}
    ]
}'); -- { serverError INCORRECT_DATA }

-- Trailing data after the top-level FeatureCollection object is rejected.
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[]} garbage'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- Trailing data after a statement terminator is also rejected (only a single trailing `;` is tolerated for inline INSERT).
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[]}; garbage'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- Malformed JSON inside an ignored field is rejected (missing comma between members of a skipped object).
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","bbox":{"a":1 "b":2},"features":[]}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- An explicit "properties": null is preserved as NULL (not silently defaulted).
SELECT isNull(properties)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": null, "properties": null}
    ]
}');

-- The top-level 'type' must be 'FeatureCollection': a wrong value is rejected.
SELECT count()
FROM format('GeoJSON', '{"type":"Feature","features":[]}'); -- { serverError INCORRECT_DATA }

-- The top-level 'type' member is required: a document without it is rejected.
SELECT count()
FROM format('GeoJSON', '{"features":[]}'); -- { serverError INCORRECT_DATA }

-- The top-level 'type' is also validated when it follows the 'features' array.
SELECT count()
FROM format('GeoJSON', '{"features":[],"type":"Feature"}'); -- { serverError INCORRECT_DATA }

-- Every member of 'features' must be a 'Feature': a wrong feature type is rejected.
SELECT count()
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "NotAFeature", "geometry": null, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A feature is required to have a 'type' member: one without it is rejected.
SELECT count()
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"geometry": null, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A LineString must have at least two positions: a one-point line is rejected.
SELECT count()
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[0,0]]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A Polygon ring must be closed: an unclosed ring is rejected.
SELECT count()
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[0,0],[1,0],[1,1],[0,1]]]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A Polygon ring must have at least four positions: a too-short ring is rejected.
SELECT count()
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[0,0],[1,1],[0,0]]]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A MultiPolygon with an unclosed ring is rejected.
SELECT count()
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "MultiPolygon", "coordinates": [[[[0,0],[1,0],[1,1],[0,1]]]]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }


-- A MultiPoint without the required 'coordinates' member is malformed input and is rejected even
-- with null handling, instead of being silently inserted as NULL.
SELECT isNull(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "MultiPoint"}, "properties": {}}
    ]
}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }

-- A GeometryCollection without the required 'geometries' member is rejected even with null handling.
SELECT isNull(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "GeometryCollection"}, "properties": {}}
    ]
}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }

-- A GeometryCollection whose 'geometries' member is not an array is malformed and is rejected even
-- with null handling, instead of being silently inserted as NULL.
SELECT isNull(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "GeometryCollection", "geometries": "not-an-array"}, "properties": {}}
    ]
}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }

-- A GeometryCollection containing a malformed child geometry (a Point missing its coordinates) is
-- rejected even with null handling, instead of being silently inserted as NULL.
SELECT isNull(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "GeometryCollection", "geometries": [{"type": "Point"}]}, "properties": {}}
    ]
}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }

-- A GeometryCollection containing a child with a bad coordinate (a non-JSON number) is rejected.
SELECT isNull(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "GeometryCollection", "geometries": [{"type": "Point", "coordinates": [+Inf, 0]}]}, "properties": {}}
    ]
}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }

-- A GeometryCollection whose members are all well-formed (including a nested collection) is still
-- inserted as NULL under null handling.
SELECT isNull(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "GeometryCollection",
                "geometries": [
                    {"type": "Point", "coordinates": [0, 0]},
                    {"type": "GeometryCollection", "geometries": [{"type": "LineString", "coordinates": [[0, 0], [1, 1]]}]}
                ]
            },
            "properties": {}
        }
    ]
}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null';

-- Coordinates must be strict JSON numbers: a non-finite value is rejected, not loaded as a Geometry.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [+Inf, 0]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A 'NaN' coordinate (a non-JSON spelling) is rejected.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [NaN, 0]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A '+1' coordinate (a non-JSON spelling with a leading plus) is rejected.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [+1, 0]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A '.5' coordinate (a non-JSON spelling missing the integer part) is rejected.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [.5, 0]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A coordinate that overflows Float64 to infinity (huge exponent) is rejected.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [1e400, 0]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A GeometryCollection whose 'geometries' array contains a JSON null is malformed: a collection
-- member must be a geometry object (only a Feature's top-level geometry may be null). It is rejected
-- even under null handling rather than being silently loaded as NULL.
SELECT isNull(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "GeometryCollection", "geometries": [null]}, "properties": {}}
    ]
}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }

-- The required 'geometry' and 'properties' members are validated even when they are not requested
-- output columns. A Feature missing them is rejected when only 'id' is selected.
SELECT id
FROM format('GeoJSON', 'id String', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1"}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A malformed 'geometry' is rejected even when 'geometry' is not a requested output column.
SELECT id
FROM format('GeoJSON', 'id String', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1", "geometry": {"type": "Point", "coordinates": [+Inf, 0]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A Feature with all required members but only 'id' selected is accepted, and the unrequested
-- 'geometry'/'properties' members are validated without being inserted.
SELECT id
FROM format('GeoJSON', 'id String', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1", "geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {}}
    ]
}');

-- A Feature's 'properties' member must be a JSON object or null. A scalar or array value is malformed
-- GeoJSON and is rejected even when 'properties' is not a requested output column (here only 'id' is
-- selected, so 'properties' goes through the validation-only path).
SELECT id
FROM format('GeoJSON', 'id String', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1", "geometry": null, "properties": 1}
    ]
}'); -- { serverError INCORRECT_DATA }

SELECT id
FROM format('GeoJSON', 'id String', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1", "geometry": null, "properties": "x"}
    ]
}'); -- { serverError INCORRECT_DATA }

SELECT id
FROM format('GeoJSON', 'id String', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1", "geometry": null, "properties": true}
    ]
}'); -- { serverError INCORRECT_DATA }

SELECT id
FROM format('GeoJSON', 'id String', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1", "geometry": null, "properties": [1, 2]}
    ]
}'); -- { serverError INCORRECT_DATA }

-- The same scalar 'properties' is rejected when 'properties' IS a requested output column.
SELECT properties
FROM format('GeoJSON', 'id String, properties Nullable(JSON)', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1", "geometry": null, "properties": 1}
    ]
}'); -- { serverError INCORRECT_DATA }

-- An explicit JSON null 'properties' is accepted on the validation-only path ('id'-only header).
SELECT id
FROM format('GeoJSON', 'id String', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1", "geometry": null, "properties": null}
    ]
}');

-- A numeric 'id' is valid per RFC 7946 and is stored verbatim as text (here 42).
SELECT id
FROM format('GeoJSON', 'id String', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": 42, "geometry": null, "properties": {}}
    ]
}');

-- A large integer 'id' keeps its exact value.
SELECT id
FROM format('GeoJSON', 'id String', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": 123456789012345678, "geometry": null, "properties": {}}
    ]
}');

-- A numeric 'id' is accepted even when reading numbers as strings is disabled.
SELECT id
FROM format('GeoJSON', 'id String', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": 42, "geometry": null, "properties": {}}
    ]
}')
SETTINGS input_format_json_read_numbers_as_strings = 0;

-- A Feature's 'id' must be a JSON string or number, so an object value is rejected.
SELECT id
FROM format('GeoJSON', 'id String', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": {"a": 1}, "geometry": null, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- An array 'id' is rejected.
SELECT id
FROM format('GeoJSON', 'id String', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": [1, 2], "geometry": null, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A boolean 'id' is rejected.
SELECT id
FROM format('GeoJSON', 'id String', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": true, "geometry": null, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A malformed 'id' is validated and rejected even when 'id' is not a requested output column.
SELECT count()
FROM format('GeoJSON', 'geometry Geometry', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": {"a": 1}, "geometry": null, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- The 'id' column must have type String.
SELECT *
FROM format('GeoJSON', 'id UInt64, geometry Geometry, properties JSON', '{"type":"FeatureCollection","features":[]}'); -- { serverError BAD_ARGUMENTS }

-- A duplicate 'geometry' member within a feature is rejected as bad input.
SELECT count()
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": null, "geometry": null, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A duplicate 'properties' member within a feature is rejected as bad input.
SELECT count()
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": null, "properties": {}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A coordinate with a trailing dot ('1.') is a non-JSON spelling and is rejected.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [1., 0]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A single trailing ';' is tolerated so a document can be supplied as inline INSERT data.
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":null,"properties":{}}]};');

-- Only the 'geometry' column may be selected.
SELECT variantType(geometry)
FROM format('GeoJSON', 'geometry Geometry', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {}}
    ]
}');

-- Only the 'properties' column may be selected.
SELECT JSONExtractString(properties, 'name') AS name
FROM format('GeoJSON', 'properties Nullable(JSON)', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": null, "properties": {"name": "Berlin"}}
    ]
}');
