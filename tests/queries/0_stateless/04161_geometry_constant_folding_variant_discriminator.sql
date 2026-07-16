-- Constant folding must preserve the Variant discriminator of a named Variant (Geometry).
-- Geometry has layout-duplicate alternatives (LineString/Ring are Array(Point),
-- MultiLineString/Polygon are Array(Array(Point)), empties are layout-compatible with all),
-- so two constants that differ only in their alternative render to the same value string.
-- They must not collapse to one action node in the DAG. See issue #110698.

SET enable_analyzer = 1;

-- Empty geometries: three distinct types that all fold to an empty array.
SELECT wkt(readWKT('LINESTRING EMPTY')), wkt(readWKT('MULTILINESTRING EMPTY')), wkt(readWKT('MULTIPOLYGON EMPTY'));
SELECT hex(wkb(readWKT('LINESTRING EMPTY'))), hex(wkb(readWKT('MULTILINESTRING EMPTY'))), hex(wkb(readWKT('MULTIPOLYGON EMPTY')));

-- Non-empty layout-duplicate alternatives with identical points.
SELECT wkt(readWKT('LINESTRING(0 0,1 0,0 0)')), wkt(readWKT('MULTILINESTRING((0 0,1 0,0 0))')), wkt(readWKT('POLYGON((0 0,1 0,0 0))'));

-- Results must match the pre-folding path (row columns) exactly.
SELECT wkt(readWKT(l)), wkt(readWKT(m)), wkt(readWKT(p))
FROM (SELECT materialize('LINESTRING EMPTY') AS l, materialize('MULTILINESTRING EMPTY') AS m, materialize('MULTIPOLYGON EMPTY') AS p);
