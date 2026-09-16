-- Tags: no-fasttest
-- no-fasttest: h3ToGeo and geoToH3 need a binary with the Uber H3 library

-- `h3togeo_lon_lat_result_order` and `geotoh3_argument_order` change the VALUE a key function produces
-- without changing its type. The stored key is built under the server baseline, while a query
-- predicate or ORDER BY over the same text is analyzed in the session. When the session deviates,
-- the index consumers must not match the two by name: pruning would drop the row the filter keeps,
-- and read-in-order would announce an order the parts do not have. See PR #109196.

DROP TABLE IF EXISTS t_h3_pk;
DROP TABLE IF EXISTS t_h3_part;
DROP TABLE IF EXISTS t_h3_minmax;
DROP TABLE IF EXISTS t_h3_geo;

-- 608009741498580991: latitude 80.007, longitude 9.99. 610315033787760639: latitude -0.0005, longitude 69.99.
CREATE TABLE t_h3_pk (h UInt64, v UInt32) ENGINE = MergeTree ORDER BY tupleElement(h3ToGeo(h), 1);
INSERT INTO t_h3_pk VALUES (608009741498580991, 1);
INSERT INTO t_h3_pk VALUES (610315033787760639, 2);

CREATE TABLE t_h3_part (h UInt64, v UInt32) ENGINE = MergeTree PARTITION BY toInt32(tupleElement(h3ToGeo(h), 1)) ORDER BY v;
INSERT INTO t_h3_part VALUES (608009741498580991, 1);
INSERT INTO t_h3_part VALUES (610315033787760639, 2);

CREATE TABLE t_h3_minmax (h UInt64, v UInt32, INDEX i_lat tupleElement(h3ToGeo(h), 1) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_h3_minmax VALUES (608009741498580991, 1);
INSERT INTO t_h3_minmax VALUES (610315033787760639, 2);

CREATE TABLE t_h3_geo (lat Float64, lon Float64, v UInt32) ENGINE = MergeTree ORDER BY geoToH3(lat, lon, 5);
INSERT INTO t_h3_geo VALUES (80.0, 10.0, 1);
INSERT INTO t_h3_geo VALUES (0.0, 70.0, 2);

SELECT '-- baseline session: the key is used and the result is right';
SELECT h FROM t_h3_pk WHERE tupleElement(h3ToGeo(h), 1) > 50;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT h FROM t_h3_pk WHERE tupleElement(h3ToGeo(h), 1) > 50) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Parts:%';
SELECT h FROM t_h3_part WHERE tupleElement(h3ToGeo(h), 1) > 50;
SELECT h FROM t_h3_minmax WHERE tupleElement(h3ToGeo(h), 1) > 50;
SELECT v FROM t_h3_geo WHERE geoToH3(lat, lon, 5) = geoToH3(80.0, 10.0, 5);
-- The two counts below only mean anything while in-order reading is enabled: with it off both are 0,
-- whatever the guard does, so the setting is pinned rather than taken from the session.
SELECT count() FROM (EXPLAIN SELECT tupleElement(h3ToGeo(h), 1) AS k FROM t_h3_pk ORDER BY k SETTINGS optimize_read_in_order = 1) WHERE explain LIKE '%Read type: InOrder%';
SELECT tupleElement(h3ToGeo(h), 1) AS k FROM t_h3_pk ORDER BY k;

SELECT '-- deviating session: element 1 is now the longitude, so the other row matches and the key must not be used';
SET h3togeo_lon_lat_result_order = 1;
SELECT h FROM t_h3_pk WHERE tupleElement(h3ToGeo(h), 1) > 50;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT h FROM t_h3_pk WHERE tupleElement(h3ToGeo(h), 1) > 50) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Parts:%';
SELECT h FROM t_h3_pk WHERE tupleElement(h3ToGeo(h), 1) > 50 SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT h FROM t_h3_part WHERE tupleElement(h3ToGeo(h), 1) > 50;
SELECT h FROM t_h3_minmax WHERE tupleElement(h3ToGeo(h), 1) > 50;
SELECT count() FROM (EXPLAIN SELECT tupleElement(h3ToGeo(h), 1) AS k FROM t_h3_pk ORDER BY k SETTINGS optimize_read_in_order = 1) WHERE explain LIKE '%Read type: InOrder%';
SELECT tupleElement(h3ToGeo(h), 1) AS k FROM t_h3_pk ORDER BY k;
SET h3togeo_lon_lat_result_order = 0;

SELECT '-- deviating session for geoToH3: the arguments are exchanged, the result type is not';
SET geotoh3_argument_order = 'lon_lat';
SELECT v FROM t_h3_geo WHERE geoToH3(lat, lon, 5) = geoToH3(80.0, 10.0, 5);
SELECT v FROM t_h3_geo WHERE geoToH3(lat, lon, 5) = geoToH3(80.0, 10.0, 5) SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SET geotoh3_argument_order = 'lat_lon';

SELECT '-- a metadata-changing ALTER under the deviating session does not exchange the stored geoToH3 key either';
ALTER TABLE t_h3_geo MODIFY COMMENT 'touch' SETTINGS geotoh3_argument_order = 'lon_lat';
INSERT INTO t_h3_geo VALUES (80.0, 10.0, 3);
SELECT v FROM t_h3_geo WHERE geoToH3(lat, lon, 5) = geoToH3(80.0, 10.0, 5) ORDER BY v;

DROP TABLE t_h3_pk;
DROP TABLE t_h3_part;
DROP TABLE t_h3_minmax;
DROP TABLE t_h3_geo;
