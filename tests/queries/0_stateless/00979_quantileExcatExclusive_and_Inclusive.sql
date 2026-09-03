DROP TABLE IF EXISTS num;
CREATE TABLE num AS numbers(1000);

SELECT quantilesExactExclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x) FROM (SELECT number AS x FROM num);
SELECT quantilesExactInclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x) FROM (SELECT number AS x FROM num);
SELECT quantilesExact(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x) FROM (SELECT number AS x FROM num);

SELECT quantileExactExclusive(0.6)(x) FROM (SELECT number AS x FROM num);
SELECT quantileExactInclusive(0.6)(x) FROM (SELECT number AS x FROM num);
SELECT quantileExact(0.6)(x) FROM (SELECT number AS x FROM num);

DROP TABLE num;
