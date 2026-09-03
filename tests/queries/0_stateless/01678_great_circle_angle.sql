SET geo_distance_returns_float64_on_float64_arguments = 0;

SELECT round(greatCircleAngle(0, 45, 0.1, 45.1), 4);
SELECT round(greatCircleAngle(0, 45, 1, 45), 4);
SELECT round(greatCircleAngle(0, 45, 1, 45.1), 4);

SELECT round(greatCircleDistance(0, 0, 0, 90), 4);
SELECT round(greatCircleDistance(0, 0, 90, 0), 4);

SET geo_distance_returns_float64_on_float64_arguments = 1;

SELECT round(greatCircleAngle(0, 45, 0.1, 45.1), 4);
SELECT round(greatCircleAngle(0, 45, 1, 45), 4);
SELECT round(greatCircleAngle(0, 45, 1, 45.1), 4);

SELECT round(greatCircleDistance(0, 0, 0, 90), 4);
SELECT round(greatCircleDistance(0, 0, 90, 0), 4);
