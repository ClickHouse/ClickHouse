-- Tags: no-fasttest

-- A vertex outside the domain H3 is defined on made the polyfill walk the whole grid at the target
-- resolution inside a single library call, which observes neither `max_execution_time` nor `KILL QUERY`.
-- The query below - as the AST fuzzer produced it from `04910_h3_array_building_no_quadratic_realloc` -
-- ran for more than 500 seconds after it had been cancelled.

SELECT sum(length(h3PolygonToCellsWithContainment([(37.81331899998324, materialize(-122.40898669999721)), (-122.35447369999936, 37.71980619999785), (9007199254740991., 100000000000000000000.)], 9, 0)))
FROM numbers(257); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Each coordinate is bounded on both sides, for both functions.

SELECT h3PolygonToCells([(0.0, 0.0), (1.0, 0.0), (180.0001, 1.0)], 7); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT h3PolygonToCells([(0.0, 0.0), (1.0, 0.0), (-180.0001, 1.0)], 7); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT h3PolygonToCells([(0.0, 0.0), (1.0, 0.0), (1.0, 90.0001)], 7); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT h3PolygonToCells([(0.0, 0.0), (1.0, 0.0), (1.0, -90.0001)], 7); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT h3PolygonToCellsWithContainment([(0.0, 0.0), (1.0, 0.0), (180.0001, 1.0)], 7, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT h3PolygonToCellsWithContainment([(0.0, 0.0), (1.0, 0.0), (1.0, 90.0001)], 7, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Coordinates that are not finite never reach H3 either - the geometry converter rejects them earlier.

SELECT h3PolygonToCells([(0.0, 0.0), (1.0, 0.0), (nan, 1.0)], 7); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT h3PolygonToCells([(0.0, 0.0), (1.0, 0.0), (1.0, inf)], 7); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT h3PolygonToCellsWithContainment([(0.0, 0.0), (1.0, 0.0), (-inf, 1.0)], 7, 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A hole of a polygon is checked too, not only its exterior ring.

SELECT h3PolygonToCells(CAST([[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)], [(0.2, 0.2), (0.2, 0.3), (200.0, 0.3), (0.2, 0.2)]], 'Polygon'), 7); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT h3PolygonToCellsWithContainment(CAST([[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)], [(0.2, 0.2), (0.2, 0.3), (200.0, 0.3), (0.2, 0.2)]], 'Polygon'), 7, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- A single out-of-bounds row rejects the query even when the other rows are fine.

SELECT length(h3PolygonToCells([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0 + number * 100.0)], 7))
FROM numbers(2); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The extremes of the domain themselves are still accepted.

SELECT length(h3PolygonToCells([(-180.0, -90.0), (-179.0, -89.0), (-179.5, -89.5)], 5)) >= 0;
SELECT length(h3PolygonToCellsWithContainment([(180.0, 90.0), (179.0, 89.0), (179.5, 89.5)], 5, 0)) >= 0;
SELECT length(h3PolygonToCellsWithContainment([(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)], 7, 0)) > 0;
