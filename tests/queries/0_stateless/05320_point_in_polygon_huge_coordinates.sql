-- Tags: no-parallel-replicas
-- Provenance: https://s3.amazonaws.com/clickhouse-test-reports/praktika.html?REF=master&sha=782be4dcec24970dad336d1af4f321c7ff53da40&name_0=MasterCI&name_1=AST%20fuzzer%20%28amd_release%2C%20oracle%29

-- A polygon with a coordinate beyond 1e100 in absolute value is rejected: a constant array polygon always, other
-- polygons when such a coordinate is on an edge evaluated for the point. Both point-in-polygon algorithms returned
-- wrong results for such polygons, and the exact primary key analysis then dropped rows the function matched.

DROP TABLE IF EXISTS pip_pk;
DROP TABLE IF EXISTS pip_nopk;
CREATE TABLE pip_pk (x Float64, y Float64) ENGINE = MergeTree ORDER BY (x, y)
    SETTINGS index_granularity = 8, index_granularity_bytes = 0;
CREATE TABLE pip_nopk (x Float64, y Float64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 8, index_granularity_bytes = 0;
INSERT INTO pip_pk SELECT (number % 100) * 0.1, (intDiv(number, 100) % 100) * 0.1 FROM numbers(10000);
INSERT INTO pip_nopk SELECT * FROM pip_pk;

-- The polygon from the fuzzer, with vertices at DBL_MAX and FLT_MAX.
SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (1.1920928955078125e-7, 1.7976931348623157e308), (0.9999, 3.4028234663852886e38), (1.0001, 0.)]); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (1.1920928955078125e-7, 1.7976931348623157e308), (0.9999, 3.4028234663852886e38), (1.0001, 0.)]); -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((0.9, 5.), [(0., 0.), (1.1920928955078125e-7, 1.7976931348623157e308), (0.9999, 3.4028234663852886e38), (1.0001, 0.)]); -- { serverError BAD_ARGUMENTS }

-- Also when the primary key excludes every granule.
SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., -10.), (0.5, -1e160), (1., -10.)]); -- { serverError BAD_ARGUMENTS }

-- Every ring is checked: a hole, a degenerate polygon, and one polygon of a multipolygon.
SET validate_polygons = 0;
SELECT pointInPolygon((0.5, 0.5), [(0., 0.), (1., 0.), (1., 1.), (0., 1.)], [(0.2, 0.2), (0.3, 0.2), (0.3, 1e200)]); -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((1., 0.), [(0., 0.), (1e200, 0.), (2e200, 0.)]); -- { serverError BAD_ARGUMENTS }
SET validate_polygons = 1;
SELECT pointInPolygon((0.5, 0.5), [[[(0., 0.), (1., 0.), (1., 1.), (0., 1.)]], [[(5., 5.), (6., 5.), (0.5, 1e200)]]]); -- { serverError BAD_ARGUMENTS }
-- A polygon huge in both axes, in the holes-as-arguments form.
SELECT pointInPolygon((-0.9623 * 1e103, -0.9571 * 1e103), [(-1e103, -9e102), (8e102, -1e103), (3e102, 9.5e102)], [(-1e102, -2e102), (1e102, -2.5e102), (5e101, -5e101)]); -- { serverError BAD_ARGUMENTS }

-- Polygons other than a plain constant array are evaluated by ray casting and are rejected too.
SELECT pointInPolygon((9e199, 5e199), CAST(CAST([(0., 0.), (1e200, 0.), (0., 1e200)] AS Ring) AS Geometry)); -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((9e199, 5e199), CAST([(0., 0.), (1e200, 0.), (0., 1e200)] AS Variant(Array(Tuple(Float64, Float64)), UInt8))); -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((9e199, 5e199), CAST([(0., 0.), (1e200, 0.), (0., 1e200)] AS Dynamic)); -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((9e199, 5e199), materialize([(0., 0.), (1e200, 0.), (0., 1e200)])); -- { serverError BAD_ARGUMENTS }
-- Each coordinate of an edge that crosses the point's ray is checked.
SELECT pointInPolygon((0., 1.), materialize([(-1e200, 0.), (1., 0.), (0., 2.)])); -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((0., 1.), materialize([(0., 2.), (1., 0.), (-1e200, 0.)])); -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((0.5, 5.), materialize([(0., 0.), (1., 0.), (1., 1e200), (0., 10.)])); -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((0.5, 5.), materialize([(0., 10.), (1., 1e200), (1., 0.), (0., 0.)])); -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((0.9, 0.5), CAST(CAST([(0., 0.), (1., 0.), (0., 1.)] AS Ring) AS Geometry)), pointInPolygon((0.9, 0.5), materialize([(0., 0.), (1., 0.), (0., 1.)]));

-- Up to 1e100 the polygon is evaluated, and primary key analysis prunes and agrees with the function.
SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (1.1920928955078125e-7, 1e100), (0.9999, 1e100), (1.0001, 0.)]) SETTINGS max_rows_to_read = 2000, use_lightweight_primary_key_index_analysis = 0, use_query_condition_cache = 0;
SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (1.1920928955078125e-7, 1e100), (0.9999, 1e100), (1.0001, 0.)]) SETTINGS max_rows_to_read = 2000, use_lightweight_primary_key_index_analysis = 1, use_query_condition_cache = 0;
SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (1.1920928955078125e-7, 1e100), (0.9999, 1e100), (1.0001, 0.)]);
SELECT pointInPolygon((1e-9, 1e99), [(0., 0.), (1.1920928955078125e-7, 1e100), (0.9999, 1e100), (1.0001, 0.)]),
       pointInPolygon((0.9, 5.), [(0., 0.), (1.1920928955078125e-7, 1e100), (0.9999, 1e100), (1.0001, 0.)]);
SELECT pointInPolygon((9e99, 5e99), materialize([(0., 0.), (1e100, 0.), (0., 1e100)])), pointInPolygon((4e99, 5e99), materialize([(0., 0.), (1e100, 0.), (0., 1e100)]));

DROP TABLE pip_pk;
DROP TABLE pip_nopk;
