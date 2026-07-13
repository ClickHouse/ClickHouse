-- Tags: no-fasttest
-- no-fasttest: h3ToGeo needs binary with Uber H3 libary

-- Test for setting 'h3togeo_lon_lat_result_order'

SELECT h3ToGeo(644325524701193974) SETTINGS h3togeo_lon_lat_result_order = true;
SELECT h3ToGeo(644325524701193974) SETTINGS h3togeo_lon_lat_result_order = false;
