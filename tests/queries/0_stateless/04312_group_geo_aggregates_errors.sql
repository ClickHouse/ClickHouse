-- Negative tests for geo aggregate functions

-- groupPolygonUnion rejects Point
SELECT groupPolygonUnion(pt) FROM (SELECT readWKTPoint('POINT (0 0)') AS pt); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- groupPolygonIntersection rejects Point
SELECT groupPolygonIntersection(pt) FROM (SELECT readWKTPoint('POINT (0 0)') AS pt); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- groupConvexHull rejects NaN
SELECT groupConvexHull(pt) FROM (SELECT (nan, 0.)::Point AS pt); -- { serverError BAD_ARGUMENTS }

-- groupConvexHull rejects Inf
SELECT groupConvexHull(pt) FROM (SELECT (inf, 0.)::Point AS pt); -- { serverError BAD_ARGUMENTS }

-- groupPolygonUnion rejects non-geo type
SELECT groupPolygonUnion(x) FROM (SELECT 42 AS x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- groupPolygonIntersection rejects non-geo type
SELECT groupPolygonIntersection(x) FROM (SELECT 42 AS x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- groupConvexHull rejects non-geo type
SELECT groupConvexHull(x) FROM (SELECT 42 AS x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Wrong number of arguments
SELECT groupConvexHull(pt, pt) FROM (SELECT readWKTPoint('POINT (0 0)') AS pt); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT groupPolygonUnion(p, p) FROM (SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT groupPolygonIntersection(p, p) FROM (SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
