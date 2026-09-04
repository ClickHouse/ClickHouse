SET geo_distance_returns_float64_on_float64_arguments = 0;

SELECT greatCircleDistance(0., 0., 0., 1.);
SELECT greatCircleDistance(0., 89., 0, 90.);

SELECT geoDistance(0., 0., 0., 1.);
SELECT geoDistance(0., 89., 0., 90.);

SELECT greatCircleDistance(0., 0., 90., 0.);
SELECT greatCircleDistance(0., 0., 0., 90.);

SELECT geoDistance(0., 0., 90., 0.);
SELECT geoDistance(0., 0., 0., 90.);

SET geo_distance_returns_float64_on_float64_arguments = 1;

SELECT greatCircleDistance(0., 0., 0., 1.);
SELECT greatCircleDistance(0., 89., 0, 90.);

SELECT geoDistance(0., 0., 0., 1.);
SELECT geoDistance(0., 89., 0., 90.);

SELECT greatCircleDistance(0., 0., 90., 0.);
SELECT greatCircleDistance(0., 0., 0., 90.);

SELECT geoDistance(0., 0., 90., 0.);
SELECT geoDistance(0., 0., 0., 90.);
