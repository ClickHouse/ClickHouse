-- Tags: no-fasttest

DROP TABLE IF EXISTS table1;

CREATE TABLE table1 (lat Float64, lon Float64, resolution UInt8) ENGINE = Memory;

INSERT INTO table1 VALUES(55.77922738, 37.63098076, 15);
INSERT INTO table1 VALUES(55.76324100, 37.66018300, 15);
INSERT INTO table1 VALUES(55.72076200, 37.59813500, 15);
INSERT INTO table1 VALUES(55.72076201, 37.59813500, 15);
INSERT INTO table1 VALUES(55.72076200, 37.59813500, 14);

select geoToH3(55.77922738, 37.63098076, 15);
select geoToH3(55.77922738, 37.63098076, 24); -- { serverError ARGUMENT_OUT_OF_BOUND }
select geoToH3(lat, lon, resolution) from table1 order by lat, lon, resolution;
select geoToH3(lat, lon, resolution) AS k from table1 order by lat, lon, k;
select lat, lon, geoToH3(lat, lon, resolution) AS k from table1 order by lat, lon, k;
select geoToH3(lat, lon, resolution) AS k, count(*) from table1 group by k order by k;

-- Latitude must be within [-90, 90] in both argument orders.
SELECT geoToH3(120., 30., 5); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT geoToH3(-120., 30., 5); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT geoToH3(30., 120., 5) SETTINGS geotoh3_argument_order = 'lon_lat'; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT geoToH3(30., -120., 5) SETTINGS geotoh3_argument_order = 'lon_lat'; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Latitude endpoints are valid, and longitude 120 is valid in either argument order.
SELECT geoToH3(90., 0., 5) != 0;
SELECT geoToH3(-90., 0., 5) != 0;
SELECT geoToH3(30., 120., 5) != 0;
SELECT geoToH3(120., 30., 5) != 0 SETTINGS geotoh3_argument_order = 'lon_lat';

-- Conditional guards must suppress invalid latitudes with the default short-circuit setting.
SELECT if(abs(lat) <= 90, geoToH3(lat, 30., 5), toUInt64(0)) != 0
FROM values('lat Float64', (30.), (120.))
ORDER BY lat
SETTINGS short_circuit_function_evaluation = 'enable';
SELECT if(abs(lat) <= 90, geoToH3(30., lat, 5), toUInt64(0)) != 0
FROM values('lat Float64', (30.), (120.))
ORDER BY lat
SETTINGS geotoh3_argument_order = 'lon_lat', short_circuit_function_evaluation = 'enable';

-- NULL rows must not validate nested values, regardless of the nullable argument.
SELECT isNull(geoToH3(nullIf(lat, 120.), 30., 5))
FROM values('lat Float64', (120.), (30.))
ORDER BY lat DESC;
SELECT isNull(geoToH3(lat, lon, 5))
FROM values('lat Float64, lon Nullable(Float64)', (120., NULL), (30., 30.))
ORDER BY lat DESC;
SELECT isNull(geoToH3(lon, lat, 5))
FROM values('lat Float64, lon Nullable(Float64)', (120., NULL), (30., 30.))
ORDER BY lat DESC
SETTINGS geotoh3_argument_order = 'lon_lat';
SELECT isNull(geoToH3(lat, 30., res))
FROM values('lat Float64, res Nullable(UInt8)', (120., NULL), (30., 5))
ORDER BY lat DESC;

-- Nonconstant, non-NULL invalid latitude still throws.
SELECT geoToH3(lat, 30., 5)
FROM values('lat Float64', (30.), (120.)); -- { serverError ARGUMENT_OUT_OF_BOUND }

DROP TABLE table1
