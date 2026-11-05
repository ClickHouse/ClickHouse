-- GeoJSON output: geometry shape and coordinate fidelity.

-- A Polygon with a hole keeps both rings.
SELECT [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)], [(2., 2.), (4., 2.), (4., 4.), (2., 4.), (2., 2.)]]::Polygon AS geometry FORMAT GeoJSON;

-- A MultiPolygon keeps every polygon (the second has a hole).
SELECT [[[(0., 0.), (2., 0.), (2., 2.), (0., 2.), (0., 0.)]], [[(5., 5.), (9., 5.), (9., 9.), (5., 9.), (5., 5.)], [(6., 6.), (7., 6.), (7., 7.), (6., 7.), (6., 6.)]]]::MultiPolygon AS geometry FORMAT GeoJSON;

-- A MultiLineString keeps every line.
SELECT [[(0., 0.), (1., 1.)], [(2., 2.), (3., 3.)]]::MultiLineString AS geometry FORMAT GeoJSON;

-- The same Array(Array(Point)) storage is emitted as Polygon vs MultiLineString depending on the type.
SELECT [[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]::Polygon AS geometry FORMAT GeoJSON;
SELECT [[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]::MultiLineString AS geometry FORMAT GeoJSON;

-- The Geometry Variant dispatches on the active type; each WKT type round-trips to its GeoJSON type.
SELECT readWKT('POINT(1 2)') AS geometry FORMAT GeoJSON;
SELECT readWKT('LINESTRING(0 0, 1 1, 2 2)') AS geometry FORMAT GeoJSON;
SELECT readWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))') AS geometry FORMAT GeoJSON;
SELECT readWKT('MULTIPOLYGON(((0 0, 2 0, 2 2, 0 2, 0 0)), ((5 5, 9 5, 9 9, 5 9, 5 5)))') AS geometry FORMAT GeoJSON;
SELECT readWKT('MULTILINESTRING((0 0, 1 1), (2 2, 3 3))') AS geometry FORMAT GeoJSON;

-- Several features, including a NULL geometry in the middle, become comma-separated Features.
SELECT geometry FROM
(
    SELECT 1 AS n, readWKT('POINT(1 2)') AS geometry
    UNION ALL SELECT 2, CAST(NULL AS Geometry)
    UNION ALL SELECT 3, readWKT('LINESTRING(0 0, 1 1)')
)
ORDER BY n
FORMAT GeoJSON;

-- Coordinate edge cases: negative, high precision, large/small magnitude, zero and whole numbers.
SELECT readWKTPoint('POINT(-122.4 37.7)') AS geometry FORMAT GeoJSON;
SELECT readWKTPoint('POINT(1.23456789012345 2.3456789012345)') AS geometry FORMAT GeoJSON;
SELECT readWKTPoint('POINT(1e10 -1e-10)') AS geometry FORMAT GeoJSON;
SELECT readWKTPoint('POINT(0 5)') AS geometry FORMAT GeoJSON;

-- Coordinates are always plain JSON numbers, even when 64-bit floats are configured to be quoted.
SELECT readWKTPoint('POINT(1.5 2.5)') AS geometry FORMAT GeoJSON SETTINGS output_format_json_quote_64bit_floats = 1;
