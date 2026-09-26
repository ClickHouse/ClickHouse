-- `geo_distance_returns_float64_on_float64_arguments` makes geoDistance, greatCircleDistance and
-- greatCircleAngle compute in single precision over Float64 arguments, so the same key text means a
-- rounded value in a session that deviates from the baseline the stored key was built under. The
-- consumers match key expressions by name, so they must not use such a key: pruning would drop the row
-- the runtime filter keeps. See PR #109196.

DROP TABLE IF EXISTS t_geo_plain;
DROP TABLE IF EXISTS t_geo_pk;
DROP TABLE IF EXISTS t_geo_minmax;
DROP TABLE IF EXISTS t_geo_set;
DROP TABLE IF EXISTS t_geo_angle;

-- Over Float64 columns geoDistance(0, 0, 20, 20) is 3112431.191311497 under the baseline and 3112431 in
-- Float32; greatCircleAngle(0, 0, 20, 20) is 27.990734623738863 and 27.990732. The rounded angle is
-- compared as Float32 because widening it back to Float64 does not give that decimal.
CREATE TABLE t_geo_plain (a Float64, b Float64, c Float64, d Float64, v UInt32) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_geo_plain VALUES (0, 0, 10, 10, 1);
INSERT INTO t_geo_plain VALUES (0, 0, 20, 20, 2);

CREATE TABLE t_geo_pk (a Float64, b Float64, c Float64, d Float64, v UInt32) ENGINE = MergeTree ORDER BY geoDistance(a, b, c, d);
INSERT INTO t_geo_pk VALUES (0, 0, 10, 10, 1);
INSERT INTO t_geo_pk VALUES (0, 0, 20, 20, 2);

CREATE TABLE t_geo_minmax (a Float64, b Float64, c Float64, d Float64, v UInt32,
    INDEX i_dist geoDistance(a, b, c, d) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_geo_minmax VALUES (0, 0, 10, 10, 1);
INSERT INTO t_geo_minmax VALUES (0, 0, 20, 20, 2);

CREATE TABLE t_geo_set (a Float64, b Float64, c Float64, d Float64, v UInt32,
    INDEX i_dist geoDistance(a, b, c, d) TYPE set(100) GRANULARITY 1) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_geo_set VALUES (0, 0, 10, 10, 1);
INSERT INTO t_geo_set VALUES (0, 0, 20, 20, 2);

CREATE TABLE t_geo_angle (a Float64, b Float64, c Float64, d Float64, v UInt32) ENGINE = MergeTree ORDER BY greatCircleAngle(a, b, c, d);
INSERT INTO t_geo_angle VALUES (0, 0, 10, 10, 1);
INSERT INTO t_geo_angle VALUES (0, 0, 20, 20, 2);

SELECT '-- baseline session: the key and both skip indexes are used and the result is right';
SELECT v FROM t_geo_pk WHERE geoDistance(a, b, c, d) = 3112431.191311497 SETTINGS force_primary_key = 1;
SELECT v FROM t_geo_minmax WHERE geoDistance(a, b, c, d) = 3112431.191311497 SETTINGS force_data_skipping_indices = 'i_dist', use_query_condition_cache = 0;
SELECT v FROM t_geo_set WHERE geoDistance(a, b, c, d) = 3112431.191311497 SETTINGS force_data_skipping_indices = 'i_dist', use_query_condition_cache = 0;
SELECT v FROM t_geo_angle WHERE greatCircleAngle(a, b, c, d) = 27.990734623738863 SETTINGS force_primary_key = 1;

SELECT '-- deviating session: the value is rounded, so the row is found only while nothing prunes by name';
SET geo_distance_returns_float64_on_float64_arguments = 0;
SELECT v FROM t_geo_plain WHERE geoDistance(a, b, c, d) = 3112431;
SELECT v FROM t_geo_pk WHERE geoDistance(a, b, c, d) = 3112431;
SELECT v FROM t_geo_pk WHERE geoDistance(a, b, c, d) = 3112431 SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT v FROM t_geo_minmax WHERE geoDistance(a, b, c, d) = 3112431 SETTINGS use_query_condition_cache = 0;
SELECT v FROM t_geo_minmax WHERE geoDistance(a, b, c, d) = 3112431 SETTINGS force_data_skipping_indices = 'i_dist'; -- { serverError INDEX_NOT_USED }
SELECT v FROM t_geo_set WHERE geoDistance(a, b, c, d) = 3112431 SETTINGS use_query_condition_cache = 0;
SELECT v FROM t_geo_set WHERE geoDistance(a, b, c, d) = 3112431 SETTINGS force_data_skipping_indices = 'i_dist'; -- { serverError INDEX_NOT_USED }
SELECT v FROM t_geo_angle WHERE greatCircleAngle(a, b, c, d) = toFloat32(27.990732);
SELECT v FROM t_geo_angle WHERE greatCircleAngle(a, b, c, d) = toFloat32(27.990732) SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SET geo_distance_returns_float64_on_float64_arguments = 1;

DROP TABLE t_geo_plain;
DROP TABLE t_geo_pk;
DROP TABLE t_geo_minmax;
DROP TABLE t_geo_set;
DROP TABLE t_geo_angle;
