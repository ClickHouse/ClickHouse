-- Tags: no-fasttest

-- Test groupPolygonUnion with Ring, Polygon, and MultiPolygon inputs

DROP TABLE IF EXISTS geo;

-- Test 1: Ring input — single ring, identity
CREATE TABLE geo (id Int32, ring Ring) ENGINE = Memory();
INSERT INTO geo VALUES (1, [(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]);
SELECT 'Ring single:', wkt(groupPolygonUnion(ring)) FROM geo;

-- Test 2: Ring input — two overlapping rings
INSERT INTO geo VALUES (2, [(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]);
SELECT 'Ring union:', wkt(groupPolygonUnion(ring)) FROM geo;
DROP TABLE geo;

-- Test 3: Polygon input — single polygon, identity
CREATE TABLE geo (id Int32, polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES (1, [[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]);
SELECT 'Polygon single:', wkt(groupPolygonUnion(polygon)) FROM geo;

-- Test 4: Polygon input — two overlapping polygons
INSERT INTO geo VALUES (2, [[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]]);
SELECT 'Polygon union:', wkt(groupPolygonUnion(polygon)) FROM geo;
DROP TABLE geo;

-- Test 5: MultiPolygon input — single multipolygon, identity
CREATE TABLE geo (id Int32, mpoly MultiPolygon) ENGINE = Memory();
INSERT INTO geo VALUES (1, [[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]]);
SELECT 'MultiPolygon single:', wkt(groupPolygonUnion(mpoly)) FROM geo;

-- Test 6: MultiPolygon input — two overlapping multipolygons
INSERT INTO geo VALUES (2, [[[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]]]);
SELECT 'MultiPolygon union:', wkt(groupPolygonUnion(mpoly)) FROM geo;
DROP TABLE geo;

-- Test 7: Return type is always MultiPolygon
CREATE TABLE geo (id Int32, ring Ring, polygon Polygon, mpoly MultiPolygon) ENGINE = Memory();
INSERT INTO geo VALUES (1, [(0, 0), (1, 0), (1, 1), (0, 0)], [[(0, 0), (1, 0), (1, 1), (0, 0)]], [[[(0, 0), (1, 0), (1, 1), (0, 0)]]]);
SELECT 'Ring type:', toTypeName(groupPolygonUnion(ring)) FROM geo;
SELECT 'Polygon type:', toTypeName(groupPolygonUnion(polygon)) FROM geo;
SELECT 'MultiPolygon type:', toTypeName(groupPolygonUnion(mpoly)) FROM geo;
DROP TABLE geo;

-- Test 8: GROUP BY with Polygon input
CREATE TABLE geo (grp Int32, polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES (1, [[(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]]);
INSERT INTO geo VALUES (1, [[(1, 1), (1, 3), (3, 3), (3, 1), (1, 1)]]);
INSERT INTO geo VALUES (2, [[(10, 10), (10, 12), (12, 12), (12, 10), (10, 10)]]);
SELECT 'Group', grp, wkt(groupPolygonUnion(polygon)) FROM geo GROUP BY grp ORDER BY grp;
DROP TABLE geo;

-- Test 9: Non-overlapping polygons stay as separate polygons in the multipolygon
CREATE TABLE geo (id Int32, polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES (1, [[(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]]);
INSERT INTO geo VALUES (2, [[(10, 10), (10, 11), (11, 11), (11, 10), (10, 10)]]);
SELECT 'Disjoint:', wkt(groupPolygonUnion(polygon)) FROM geo;
DROP TABLE geo;