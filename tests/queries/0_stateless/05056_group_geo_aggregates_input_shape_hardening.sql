-- Direct polygonal input must establish the same shape-to-point relationship as serialized
-- state before allocating Boost geometry containers. In particular, empty holes must not let a
-- raw ring count allocate independently of the point budget.

SELECT 'union_empty_inner_ring_direct';
SELECT groupPolygonUnion(p)
FROM
(
    SELECT [[(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)], []]::Polygon AS p
); -- { serverError BAD_ARGUMENTS }

SELECT 'intersection_empty_inner_ring_direct';
SELECT groupPolygonIntersection(p)
FROM
(
    SELECT [[(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)], []]::Polygon AS p
); -- { serverError BAD_ARGUMENTS }

SELECT 'union_empty_inner_ring_in_multipolygon';
SELECT groupPolygonUnion(mp)
FROM
(
    SELECT [[[(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)], []]]::MultiPolygon AS mp
); -- { serverError BAD_ARGUMENTS }

-- An open three-point triangle becomes a valid four-point ring after `boost::geometry::correct`.
-- The eager shape guard must account for that closing point rather than reject repairable input.
SELECT 'union_open_triangle_still_accepted';
SELECT round(polygonAreaCartesian(groupPolygonUnion(r)), 2)
FROM
(
    SELECT [(0., 0.), (0., 1.), (1., 0.)]::Ring AS r
);
