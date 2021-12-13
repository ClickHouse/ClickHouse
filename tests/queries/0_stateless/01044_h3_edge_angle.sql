-- Tags: no-unbundled, no-fasttest

SELECT h3EdgeAngle(10);
SELECT h3EdgeLengthM(2) * 180 / pi() / 6371007.180918475 - h3EdgeAngle(2);
