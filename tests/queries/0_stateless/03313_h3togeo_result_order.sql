-- Tags: no-fasttest
-- no-fasttest: h3ToGeo needs binary with Uber H3 libary

-- Test for setting 'h3togeo_lon_lat_result_order'
-- The coordinates are rounded because the h3 library is built with scoped FP optimizations
-- (`-fassociative-math -fno-signed-zeros`) whose last-bit result is not bit-identical across
-- builds/platforms. This test only checks the lon/lat result order, not coordinate precision.

SELECT (round(t.1, 10), round(t.2, 10)) FROM (SELECT h3ToGeo(644325524701193974) AS t) SETTINGS h3togeo_lon_lat_result_order = true;
SELECT (round(t.1, 10), round(t.2, 10)) FROM (SELECT h3ToGeo(644325524701193974) AS t) SETTINGS h3togeo_lon_lat_result_order = false;
