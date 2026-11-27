-- { echoOn }
-- Reading a MultiPoint geometry from GeoJSON.
SELECT geometry, variantType(geometry) FROM format(GeoJSON, 'geometry Geometry', '{"type":"FeatureCollection","features":[
    {"type":"Feature","geometry":{"type":"MultiPoint","coordinates":[[1,2],[3,4]]},"properties":{}},
    {"type":"Feature","geometry":{"type":"MultiPoint","coordinates":[[5,6]]},"properties":{}},
    {"type":"Feature","geometry":{"type":"Point","coordinates":[7,8]},"properties":{}}
]}');

-- An empty coordinates array is valid for MultiPoint.
SELECT geometry, variantType(geometry) FROM format(GeoJSON, 'geometry Geometry', '{"type":"FeatureCollection","features":[
    {"type":"Feature","geometry":{"type":"MultiPoint","coordinates":[]},"properties":{}}
]}');

-- Writing MultiPoint geometries, both from a Geometry column and from a concrete MultiPoint column.
SELECT [(1., 2.), (3., 4.)]::MultiPoint::Geometry AS geometry FORMAT GeoJSON;
SELECT [(1., 2.), (3., 4.)]::MultiPoint AS geometry, 42 AS id FORMAT GeoJSON;
SELECT []::MultiPoint AS geometry FORMAT GeoJSON;
