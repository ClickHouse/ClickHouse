-- Tags: no-fasttest
-- no-fasttest: h3ToGeo needs binary with Uber H3 libary

-- Test for setting 'geotoh3_argument_order'

SELECT geoToH3(37.79506683, 55.71290588, 15) AS h3Index SETTINGS geotoh3_argument_order = 'lon_lat';
SELECT geoToH3(55.71290588, 37.79506683, 15) AS h3Index SETTINGS geotoh3_argument_order = 'lat_lon';
