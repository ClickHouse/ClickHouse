SELECT floor(greatCircleDistance(33.3, 55.3, 38.7, 55.1)) AS distance;
SELECT floor(greatCircleDistance(33.3 + v, 55.3 + v, 38.7 + v , 55.1 + v)) AS distance from
(
	select number + 0.1 as v from system.numbers limit 1
);
SELECT floor(greatCircleDistance(33.3, 55.3, 33.3, 55.3)) AS distance;
