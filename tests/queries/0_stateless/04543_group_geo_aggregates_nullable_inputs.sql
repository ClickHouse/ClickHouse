-- Regression test for the Nullable-specific behavior of the geo aggregate functions.
-- The polygonal argument types (Ring, Polygon, MultiPolygon) are Array-based and cannot be
-- wrapped in Nullable, but Point is a Tuple and Nullable(Point) is constructible in expressions.
-- Because the result types (Ring, MultiPolygon) cannot be inside Nullable, the Null combinator
-- keeps the non-nullable result type: a group with only NULL values yields the empty geometry,
-- consistent with the empty-group result. A literal NULL argument yields NULL, following the
-- standard aggregate convention (like sum and unlike count).

-- 1. Nullable(Point): NULL rows are skipped, the hull is built from the non-NULL points
SELECT 'convex_hull_nullable_point_skips_nulls';
SELECT wkt(groupConvexHull(p)) FROM
(
    SELECT arrayJoin([
        toNullable((0., 0.)::Point),
        NULL,
        toNullable((4., 0.)::Point),
        NULL,
        toNullable((2., 3.)::Point)
    ]) AS p
);

-- 2. Nullable(Point): the result type stays a plain Ring (a Ring cannot be inside Nullable)
SELECT 'convex_hull_nullable_point_result_type';
SELECT toTypeName(groupConvexHull(toNullable((0., 0.)::Point)));

-- 3. Nullable(Point): a group with only NULL values yields an empty Ring, like an empty group
SELECT 'convex_hull_nullable_point_all_null';
SELECT groupConvexHull(if(number < 0, toNullable((0., 0.)::Point), NULL)) FROM numbers(3);

-- 4. Nullable(Point): an empty group yields an empty Ring as well
SELECT 'convex_hull_nullable_point_empty_group';
SELECT groupConvexHull(toNullable((0., 0.)::Point)) FROM numbers(10) WHERE number < 0;

-- 5. GROUP BY: all-NULL groups yield an empty Ring, mixed groups use the non-NULL points
SELECT 'convex_hull_nullable_point_group_by';
SELECT g, wkt(groupConvexHull(p))
FROM
(
    SELECT number % 2 AS g, if(number % 2 = 0, NULL, toNullable((number::Float64, 0.)::Point)) AS p
    FROM numbers(6)
)
GROUP BY g
ORDER BY g;

-- 6. A literal NULL argument yields NULL for all three functions
SELECT 'literal_null_argument';
SELECT groupPolygonUnion(NULL), groupPolygonIntersection(NULL), groupConvexHull(NULL);

-- 7. The polygonal argument types cannot be wrapped in Nullable at all
SELECT 'nullable_ring_is_not_constructible';
SELECT toNullable([(0., 0.), (1., 0.), (1., 1.)]::Ring); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
