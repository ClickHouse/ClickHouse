-- Exercise exact typed geo columns instead of only structural literals / `Geometry`.

DROP TABLE IF EXISTS geo_exact_inputs;
CREATE TABLE geo_exact_inputs
(
    ring Ring,
    ls LineString,
    mls MultiLineString,
    poly Polygon,
    mpoly MultiPolygon
)
ENGINE = Memory;

INSERT INTO geo_exact_inputs VALUES
(
    [(0., 0.)::Point, (2., 0.)::Point, (2., 2.)::Point, (0., 2.)::Point, (0., 0.)::Point],
    [(0., 0.)::Point, (3., 0.)::Point, (3., 3.)::Point, (0., 3.)::Point, (0., 0.)::Point],
    [[(0., 0.)::Point, (4., 0.)::Point, (4., 4.)::Point], [(0., 4.)::Point, (0., 0.)::Point]],
    [[(0., 0.)::Point, (5., 0.)::Point, (5., 5.)::Point, (0., 5.)::Point, (0., 0.)::Point], [(1., 1.)::Point, (1., 2.)::Point, (2., 2.)::Point, (2., 1.)::Point, (1., 1.)::Point]],
    [[[(0., 0.)::Point, (1., 0.)::Point, (1., 1.)::Point, (0., 1.)::Point, (0., 0.)::Point]], [[(3., 0.)::Point, (4., 0.)::Point, (4., 1.)::Point, (3., 1.)::Point, (3., 0.)::Point]]]
);

SELECT 'convex_hull_exact_typed_inputs';
SELECT
    round(polygonAreaCartesian(groupConvexHull(ring)), 2),
    round(polygonAreaCartesian(groupConvexHull(ls)), 2),
    round(polygonAreaCartesian(groupConvexHull(mls)), 2),
    round(polygonAreaCartesian(groupConvexHull(poly)), 2),
    round(polygonAreaCartesian(groupConvexHull(mpoly)), 2)
FROM geo_exact_inputs;

DROP TABLE geo_exact_inputs;

DROP TABLE IF EXISTS ring_empty_test;
CREATE TABLE ring_empty_test (r Ring) ENGINE = Memory;

INSERT INTO ring_empty_test VALUES
    ([]),
    ([(0., 0.)::Point, (2., 0.)::Point, (2., 2.)::Point, (0., 2.)::Point, (0., 0.)::Point]);

SELECT 'typed_ring_empty_semantics';
SELECT
    round(polygonAreaCartesian(groupPolygonUnion(r)), 2),
    round(polygonAreaCartesian(groupPolygonIntersection(r)), 2)
FROM ring_empty_test;

DROP TABLE ring_empty_test;
