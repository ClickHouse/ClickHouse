-- Negative tests for polygonal Geometry values and invalid polygon input

-- groupPolygonUnion rejects non-polygonal Geometry values
SELECT groupPolygonUnion(g) FROM
(
    SELECT readWKT('POINT (0 0)') AS g
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- groupPolygonIntersection rejects non-polygonal Geometry values
SELECT groupPolygonIntersection(g) FROM
(
    SELECT readWKT('POINT (0 0)') AS g
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- groupPolygonUnion rejects invalid Polygon input
SELECT groupPolygonUnion(p) FROM
(
    SELECT readWKTPolygon('POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))') AS p
); -- { serverError BAD_ARGUMENTS }

-- groupPolygonIntersection rejects invalid Polygon input
SELECT groupPolygonIntersection(p) FROM
(
    SELECT readWKTPolygon('POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))') AS p
); -- { serverError BAD_ARGUMENTS }

-- groupPolygonUnion rejects invalid polygonal Geometry values
SELECT groupPolygonUnion(g) FROM
(
    SELECT readWKT('POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))') AS g
); -- { serverError BAD_ARGUMENTS }

-- groupPolygonIntersection rejects invalid polygonal Geometry values
SELECT groupPolygonIntersection(g) FROM
(
    SELECT readWKT('POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))') AS g
); -- { serverError BAD_ARGUMENTS }
