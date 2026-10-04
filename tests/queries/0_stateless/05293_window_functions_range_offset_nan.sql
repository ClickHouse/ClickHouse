-- RANGE offsets over a float sort key that holds NaN and infinities.

SELECT '-- Float64 ASC, 1 PRECEDING AND 1 FOLLOWING';
SELECT x, count() OVER w, groupArray(x) OVER w
FROM values('x Float64', (0), (1), (1.5), (2), (nan), (inf), (-inf), (nan))
WINDOW w AS (ORDER BY x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
ORDER BY x, count() OVER w;

SELECT '-- Float64 ASC, CURRENT ROW AND 1 FOLLOWING';
SELECT x, count() OVER w, groupArray(x) OVER w
FROM values('x Float64', (0), (1), (1.5), (2), (nan), (inf), (-inf), (nan))
WINDOW w AS (ORDER BY x RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING)
ORDER BY x, count() OVER w;

SELECT '-- Float64 ASC, 1 PRECEDING AND CURRENT ROW';
SELECT x, count() OVER w, groupArray(x) OVER w
FROM values('x Float64', (0), (1), (1.5), (2), (nan), (inf), (-inf), (nan))
WINDOW w AS (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY x, count() OVER w;

SELECT '-- Float32 ASC, 1 PRECEDING AND 1 FOLLOWING';
SELECT x, count() OVER w, groupArray(x) OVER w
FROM values('x Float32', (0), (1), (1.5), (2), (nan), (inf))
WINDOW w AS (ORDER BY x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
ORDER BY x, count() OVER w;

SELECT '-- Nullable(Float64) ASC NULLS LAST, 1 PRECEDING AND 1 FOLLOWING';
SELECT x, count() OVER w, groupArray(x) OVER w
FROM values('x Nullable(Float64)', (0), (1), (1.5), (2), (nan), (NULL), (inf), (NULL), (nan))
WINDOW w AS (ORDER BY x ASC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
ORDER BY x ASC NULLS LAST, count() OVER w;
