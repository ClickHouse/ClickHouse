SELECT '1';
SELECT quantilesTDigestArray(0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)(arrayResize(arrayResize([inf], 500000, -inf), 1000000, inf));
SELECT quantilesTDigestArray(0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)(arrayResize(arrayResize([inf], 500000, inf), 1000000, -inf));
SELECT quantilesTDigestArray(0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)(arrayResize(arrayResize([inf], 500000, inf), 1000000, 0));
SELECT quantilesTDigestArray(0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)(arrayResize(arrayResize([inf], 500000, -inf), 1000000, 0));
SELECT quantilesTDigestArray(0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)(arrayResize(arrayResize([0], 500000, inf), 1000000, -inf));
SELECT quantilesTDigestArray(0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)(arrayResize(arrayResize([0], 500000, -inf), 1000000, inf));

SELECT '2';
SELECT quantilesTDigest(0.05)(x) FROM (SELECT inf*(number%2-0.5) x FROM numbers(300));
SELECT quantilesTDigest(0.5)(x) FROM (SELECT inf*(number%2-0.5) x FROM numbers(300));
SELECT quantilesTDigest(0.95)(x) FROM (SELECT inf*(number%2-0.5) x FROM numbers(300));

SELECT '3';
SELECT quantiles(0.5)(inf) FROM numbers(5);
SELECT quantiles(0.5)(inf) FROM numbers(300);
SELECT quantiles(0.5)(-inf) FROM numbers(5);
SELECT quantiles(0.5)(-inf) FROM numbers(300);

SELECT '4';
SELECT quantiles(0.5)(arrayJoin([inf, 0, -inf]));
SELECT quantiles(0.5)(arrayJoin([-inf, 0, inf]));
SELECT quantiles(0.5)(arrayJoin([inf, -inf, 0]));
SELECT quantiles(0.5)(arrayJoin([-inf, inf, 0]));
SELECT quantiles(0.5)(arrayJoin([inf, inf, 0, -inf, -inf, -0]));
SELECT quantiles(0.5)(arrayJoin([inf, -inf, 0, -inf, inf, -0]));
SELECT quantiles(0.5)(arrayJoin([-inf, -inf, 0, inf, inf, -0]));

SELECT '5';
DROP TABLE IF EXISTS issue32107;
CREATE TABLE issue32107(A Int64, s_quantiles AggregateFunction(quantilesTDigest(0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99), Float64)) ENGINE = AggregatingMergeTree ORDER BY A;
INSERT INTO issue32107 SELECT A, quantilesTDigestState(0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99)(x) FROM (SELECT 1 A, arrayJoin(cast([2.0, inf, number / 33333],'Array(Float64)')) x FROM numbers(100)) GROUP BY A;
OPTIMIZE TABLE issue32107 FINAL;
DROP TABLE IF EXISTS issue32107;

SELECT '6';
SELECT quantileTDigest(inf) FROM numbers(200);
SELECT quantileTDigest(inf) FROM numbers(500);
SELECT quantileTDigest(-inf) FROM numbers(200);
SELECT quantileTDigest(-inf) FROM numbers(500);

SELECT '7';
SELECT quantileTDigest(x) FROM (SELECT inf AS x UNION ALL SELECT -inf);
SELECT quantileTDigest(x) FROM (SELECT -inf AS x UNION ALL SELECT inf);

SELECT '8';
SELECT quantileTDigest(x) FROM (SELECT inf AS x UNION ALL SELECT -inf UNION ALL SELECT -inf);
SELECT quantileTDigest(x) FROM (SELECT inf AS x UNION ALL SELECT inf UNION ALL SELECT -inf);
SELECT quantileTDigest(x) FROM (SELECT -inf AS x UNION ALL SELECT -inf UNION ALL SELECT -inf);
SELECT quantileTDigest(x) FROM (SELECT -inf AS x UNION ALL SELECT inf UNION ALL SELECT -inf);
SELECT quantileTDigest(x) FROM (SELECT inf AS x UNION ALL SELECT -inf UNION ALL SELECT inf);
SELECT quantileTDigest(x) FROM (SELECT inf AS x UNION ALL SELECT inf UNION ALL SELECT inf);
SELECT quantileTDigest(x) FROM (SELECT -inf AS x UNION ALL SELECT -inf UNION ALL SELECT inf);
SELECT quantileTDigest(x) FROM (SELECT -inf AS x UNION ALL SELECT inf UNION ALL SELECT inf);
