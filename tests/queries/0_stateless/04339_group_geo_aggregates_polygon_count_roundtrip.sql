-- Regression: a polygonal aggregate state with more polygons (or inner rings) than the old
-- per-container deserialization caps (10,000) must round-trip through serialization.
--
-- The caps (MAX_POLYGONS_PER_MULTIPOLYGON, MAX_RINGS_PER_POLYGON) were enforced only on
-- deserialization, while the add/merge paths enforced just the cumulative point budget. A group
-- of many disjoint polygons therefore produced a valid state that groupPolygonUnionState /
-- groupPolygonIntersectionState could serialize, but the corresponding ...Merge rejected as
-- INCORRECT_DATA. States are now bounded by the cumulative point/ring budgets alone, so any shape
-- the aggregate can build round-trips.

-- 1. groupPolygonUnion: a single MultiPolygon of 10,001 disjoint unit squares. More than the old
--    10,000-polygon cap, far below the point budget. The union of already-disjoint polygons is
--    unchanged, so the State/Merge round-trip must return all 10,001 polygons.
SELECT 'union_many_polygons_roundtrip';
SELECT length(groupPolygonUnionMerge(state)) FROM (
    SELECT CAST(unhex(hex(groupPolygonUnionState(mp))) AS AggregateFunction(groupPolygonUnion, MultiPolygon)) AS state FROM (
        SELECT arrayMap(i -> [[
            (toFloat64(i) * 3, 0.), (toFloat64(i) * 3, 1.),
            (toFloat64(i) * 3 + 1, 1.), (toFloat64(i) * 3 + 1, 0.),
            (toFloat64(i) * 3, 0.)
        ]], range(10001))::MultiPolygon AS mp
    )
);

-- 2. groupPolygonIntersection: the same 10,001 disjoint squares. Intersection of a single input is
--    the input itself, so the round-tripped state also keeps all 10,001 polygons.
SELECT 'intersect_many_polygons_roundtrip';
SELECT length(groupPolygonIntersectionMerge(state)) FROM (
    SELECT CAST(unhex(hex(groupPolygonIntersectionState(mp))) AS AggregateFunction(groupPolygonIntersection, MultiPolygon)) AS state FROM (
        SELECT arrayMap(i -> [[
            (toFloat64(i) * 3, 0.), (toFloat64(i) * 3, 1.),
            (toFloat64(i) * 3 + 1, 1.), (toFloat64(i) * 3 + 1, 0.),
            (toFloat64(i) * 3, 0.)
        ]], range(10001))::MultiPolygon AS mp
    )
);

-- 3. groupPolygonUnion: a single Polygon with 10,001 holes. More than the old 10,000-ring cap.
--    The holes are disjoint squares laid out on a grid fully inside a large outer square, so the
--    polygon is valid and its State/Merge round-trip must preserve the outer ring plus every hole
--    (1 + 10,001 = 10,002 rings).
SELECT 'union_many_rings_roundtrip';
SELECT length(arrayElement(groupPolygonUnionMerge(state), 1)) FROM (
    SELECT CAST(unhex(hex(groupPolygonUnionState(poly))) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state FROM (
        SELECT arrayConcat(
            [[(0., 0.), (0., 110000.), (110000., 110000.), (110000., 0.), (0., 0.)]],
            arrayMap(i -> [
                (toFloat64(i % 100) * 1000 + 400, toFloat64(intDiv(i, 100)) * 1000 + 400),
                (toFloat64(i % 100) * 1000 + 400, toFloat64(intDiv(i, 100)) * 1000 + 600),
                (toFloat64(i % 100) * 1000 + 600, toFloat64(intDiv(i, 100)) * 1000 + 600),
                (toFloat64(i % 100) * 1000 + 600, toFloat64(intDiv(i, 100)) * 1000 + 400),
                (toFloat64(i % 100) * 1000 + 400, toFloat64(intDiv(i, 100)) * 1000 + 400)
            ], range(10001))
        )::Polygon AS poly
    )
);
