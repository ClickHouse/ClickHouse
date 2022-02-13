
SELECT pointInEllipses(33.3, 55.3, 33.4, 55.1, 1.0, 1.0) AS distance;
SELECT pointInEllipses(33.3 + v, 55.3 + v, 33.4, 55.1, 1.0, 1.0) AS distance from
(
	select number + 0.1 as v from system.numbers limit 1
);
SELECT pointInEllipses(33.3, 55.3, 33.4, 55.1, 0.1, 0.2) AS distance;
