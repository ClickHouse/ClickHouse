-- test data acquired with: https://github.com/sunng87/node-geohash
--  geohash.bboxes(minlat, minlon, maxlat, maxlon, precision)
-- as
-- geohashesInBox(minlon, minlat, maxlon, maxlat, precision)
-- except for the cases when JS-version produces result outside of given region,
-- typically at wrap points: poles, 0-latitude and 0-longitude.

SELECT 'center';
SELECT arraySort(geohashesInBox(-1.0, -1.0, 1.0, 1.0, 3));
SELECT arraySort(geohashesInBox(-0.1, -0.1, 0.1, 0.1, 5));
SELECT arraySort(geohashesInBox(-0.01, -0.01, 0.01, 0.01, 5));

SELECT 'north pole';
SELECT arraySort(geohashesInBox(-180.0, 89.0, -179.0, 90.0, 3));
SELECT arraySort(geohashesInBox(-1.0,   89.0, 0.0, 90.0, 3));
SELECT arraySort(geohashesInBox(0.0,    89.0, 1.0, 90.0, 3));
SELECT arraySort(geohashesInBox(179.0,  89.0, 180.0, 90.0, 3));

SELECT 'south pole';
SELECT arraySort(geohashesInBox(-180.0, -90.0, -179.0, -89.0, 3));
SELECT arraySort(geohashesInBox(-1.0,   -90.0, 0.0,    -89.0, 3));
SELECT arraySort(geohashesInBox(0.0,    -90.0, 1.0,    -89.0, 3));
SELECT arraySort(geohashesInBox(179.0,  -90.0, 180.0,  -89.0, 3));

SELECT 'wrap point around equator';
SELECT arraySort(geohashesInBox(179.0,  -1.0, 180.0,  0.0, 3));
SELECT arraySort(geohashesInBox(179.0,  0.0,  180.0,  1.0, 3));
SELECT arraySort(geohashesInBox(-180.0, -1.0, -179.0, 0.0, 3));
SELECT arraySort(geohashesInBox(-180.0, 0.0,  -179.0, 1.0, 3));

SELECT 'arbitrary values in all 4 quarters';
SELECT arraySort(geohashesInBox(98.36, 7.88, 98.37, 7.89, 6));
SELECT arraySort(geohashesInBox(53.8, 27.6, 53.9, 27.7, 5));
SELECT arraySort(geohashesInBox(-49.26, -25.38, -49.25, -25.37, 6));
SELECT arraySort(geohashesInBox(23.11, -82.37, 23.12, -82.36, 6));

SELECT 'small range always produces array of length 1';
SELECT lon/5 - 180 AS lon1, lat/5 - 90 AS lat1, lon1 AS lon2, lat1 AS lat2, geohashesInBox(lon1, lat1, lon2, lat2, 1)  AS g
FROM (SELECT arrayJoin(range(360*5)) AS lon,  arrayJoin(range(180*5)) AS lat) WHERE length(g) != 1;

SELECT lon/5 - 40 AS lon1, lat/5 - 20 AS lat1, lon1 AS lon2, lat1 AS lat2, geohashesInBox(lon1, lat1, lon2, lat2, 12) AS g
FROM (SELECT arrayJoin(range(80*5)) AS lon,  arrayJoin(range(10*5)) AS lat) WHERE length(g) != 1;

SELECT lon/5 - 40 AS lon1, lat/5 - 20 AS lat1, lon1 + 0.0000000001 AS lon2, lat1 + 0.0000000001 AS lat2, geohashesInBox(lon1, lat1, lon2, lat2, 1) AS g
FROM (SELECT arrayJoin(range(80*5)) AS lon,  arrayJoin(range(10*5)) AS lat) WHERE length(g) != 1;

SELECT 'zooming';
SELECT arraySort(geohashesInBox(20.0, 20.0, 21.0, 21.0, 2));
SELECT arraySort(geohashesInBox(20.0, 20.0, 21.0, 21.0, 3));
SELECT arraySort(geohashesInBox(20.0, 20.0, 21.0, 21.0, 4));
SELECT arraySort(geohashesInBox(20.0, 20.0, 20.25, 20.25, 5));
SELECT arraySort(geohashesInBox(20.0, 20.0, 20.0625, 20.0625, 6));
SELECT arraySort(geohashesInBox(20.0, 20.0, 20.01, 20.01, 7));
SELECT arraySort(geohashesInBox(20.0, 20.0, 20.001, 20.001, 8));
SELECT arraySort(geohashesInBox(20.0, 20.0, 20.0001, 20.0001, 9));
SELECT arraySort(geohashesInBox(20.0, 20.0, 20.00001, 20.00001, 10));
SELECT arraySort(geohashesInBox(20.0, 20.0, 20.000001, 20.000001, 11));
SELECT arraySort(geohashesInBox(20.0, 20.0, 20.000001, 20.000001, 12));

 -- precision greater than 12 is truncated to 12, so these two calls would produce same result as above
SELECT arraySort(geohashesInBox(20.0, 20.0, 20.000001, 20.000001, 13));
SELECT arraySort(geohashesInBox(20.0, 20.0, 20.000001, 20.000001, 14));

SELECT 'input values are clamped to -90..90, -180..180 range';
SELECT length(geohashesInBox(-inf, -inf, inf, inf, 3));

SELECT 'errors';
SELECT geohashesInBox();  -- { serverError 42 } -- not enough arguments
SELECT geohashesInBox(1, 2, 3, 4, 5);  -- { serverError 43 }  -- wrong types of arguments
SELECT geohashesInBox(toFloat32(1.0), 2.0, 3.0, 4.0, 5);  -- { serverError 43 } -- all lats and longs should be of the same type
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 12); -- { serverError 128 } -- to many elements in array

