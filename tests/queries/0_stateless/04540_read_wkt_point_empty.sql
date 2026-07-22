-- Tags: no-fasttest

-- "POINT EMPTY" is valid WKT but a ClickHouse Point has no empty representation.
-- It must be rejected instead of returning uninitialized (scalar) or stale prior-row (vectorized) coordinates.
SELECT readWKTPoint('POINT EMPTY'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTPoint('point empty'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTPoint('POINT   EMPTY'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTPoint(' POINT EMPTY '); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKT('POINT EMPTY'); -- { serverError CANNOT_PARSE_TEXT }
-- The generic readWKT dispatch must also reject leading-whitespace / case variants of POINT EMPTY.
SELECT readWKT(' POINT EMPTY '); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKT('point   empty'); -- { serverError CANNOT_PARSE_TEXT }

-- Valid points are unaffected.
SELECT readWKTPoint('POINT (1.2 3.4)');
SELECT readWKT('POINT (5 6)');

-- Vectorized: an empty-point row must not silently carry over prior-row coordinates; it errors the query.
DROP TABLE IF EXISTS geo_point_empty;
CREATE TABLE geo_point_empty (s String, id Int) engine=Memory();
INSERT INTO geo_point_empty VALUES ('POINT (11 22)', 1), ('POINT EMPTY', 2), ('POINT (33 44)', 3);
SELECT readWKTPoint(s) FROM geo_point_empty ORDER BY id; -- { serverError CANNOT_PARSE_TEXT }
DROP TABLE geo_point_empty;

-- Non-point empty geometries remain valid.
SELECT readWKTLineString('LINESTRING EMPTY');
SELECT readWKTPolygon('POLYGON EMPTY');
SELECT readWKTMultiPolygon('MULTIPOLYGON EMPTY');
