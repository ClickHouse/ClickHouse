-- Foreign/ignored members in a Feature, a geometry object, and the top-level
-- FeatureCollection (after the features array) are skipped, and the document is still accepted.

-- A per-feature foreign member (such as the RFC 7946 'bbox') before 'geometry' is ignored.
SELECT id
FROM format('GeoJSON', 'id String', '{"type":"FeatureCollection","features":[{"type":"Feature","id":"y","bbox":[0,0,1,1],"geometry":null,"properties":{}}]}');

-- A foreign member inside a geometry object is ignored.
SELECT geometry.Point
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[1,1],"extra":5},"properties":{}}]}');

-- A foreign member after the features array (such as 'name'/'crs') is ignored.
SELECT id
FROM format('GeoJSON', 'id String', '{"type":"FeatureCollection","features":[{"type":"Feature","id":"x","geometry":null,"properties":{}}],"name":"layer1","crs":{"type":"name"}}');
