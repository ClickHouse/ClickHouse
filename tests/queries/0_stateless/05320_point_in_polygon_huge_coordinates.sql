-- Tags: no-parallel-replicas
-- Provenance: https://s3.amazonaws.com/clickhouse-test-reports/praktika.html?REF=master&sha=782be4dcec24970dad336d1af4f321c7ff53da40&name_0=MasterCI&name_1=AST%20fuzzer%20%28amd_release%2C%20oracle%29

-- A constant polygon with a coordinate beyond 1e150 in absolute value is rejected: the grid of the
-- function misclassified whole cells for such polygons, and the exact primary key analysis then
-- dropped rows the function matched.

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

-- Every ring is checked: a hole, and one polygon of a multipolygon.
SET validate_polygons = 0;
SELECT pointInPolygon((0.5, 0.5), [(0., 0.), (1., 0.), (1., 1.), (0., 1.)], [(0.2, 0.2), (0.3, 0.2), (0.3, 1e200)]); -- { serverError BAD_ARGUMENTS }
SET validate_polygons = 1;
SELECT pointInPolygon((0.5, 0.5), [[[(0., 0.), (1., 0.), (1., 1.), (0., 1.)]], [[(5., 5.), (6., 5.), (0.5, 1e200)]]]); -- { serverError BAD_ARGUMENTS }

-- Up to 1e150 the polygon is evaluated, and primary key analysis prunes and agrees with the function.
SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (1.1920928955078125e-7, 1e150), (0.9999, 1e150), (1.0001, 0.)]) SETTINGS max_rows_to_read = 2000;
SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (1.1920928955078125e-7, 1e150), (0.9999, 1e150), (1.0001, 0.)]);
SELECT pointInPolygon((1e-9, 1e149), [(0., 0.), (1.1920928955078125e-7, 1e150), (0.9999, 1e150), (1.0001, 0.)]),
       pointInPolygon((0.9, 5.), [(0., 0.), (1.1920928955078125e-7, 1e150), (0.9999, 1e150), (1.0001, 0.)]);

DROP TABLE pip_pk;
DROP TABLE pip_nopk;
