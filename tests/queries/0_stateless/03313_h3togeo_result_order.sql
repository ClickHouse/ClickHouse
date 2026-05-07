-- Tags: no-fasttest
-- no-fasttest: h3ToGeo needs binary with Uber H3 libary

-- Test for setting 'h3togeo_lon_lat_result_order'
-- Round the coordinates so the test is robust against last-ULP differences
-- between h3o builds (e.g. coverage vs. release).

SELECT (round(t.1, 10), round(t.2, 10)) FROM (SELECT h3ToGeo(644325524701193974) AS t SETTINGS h3togeo_lon_lat_result_order = true);
SELECT (round(t.1, 10), round(t.2, 10)) FROM (SELECT h3ToGeo(644325524701193974) AS t SETTINGS h3togeo_lon_lat_result_order = false);
