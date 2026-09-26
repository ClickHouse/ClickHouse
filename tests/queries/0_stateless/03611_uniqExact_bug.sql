set max_threads=4;

create table test(c1 Int64, c2 Int64)
Engine=MergeTree ORDER BY c2 AS  
WITH gen as 
  (SELECT xxHash32(number)   % 1000000 AS u1,
          xxHash32(number+12345) % 1000000 AS u2
   FROM numbers(1e6))
SELECT
    1071106,
    5+(1 + 151703 * ((2 * u1) * (2 * u2)))
FROM gen
union all
SELECT
    1071102,
    8+(1 + 151693 * ((2 * u1) * (2 * u2)))
FROM gen;

select '--- ROLLUP ---' format TSVRaw;
SELECT
    c1,
    uniqExact(c2)
FROM test
GROUP BY c1
    WITH ROLLUP
ORDER BY c1 DESC;

select '--- CUBE ---' format TSVRaw;

SELECT
    c1,
    uniqExact(c2)
FROM test
GROUP BY c1
    WITH CUBE
ORDER BY c1 DESC;

select '--- totals ---' format TSVRaw;

SELECT
    c1,
    uniqExact(c2)
FROM test
GROUP BY c1
    WITH TOTALS
ORDER BY c1 DESC;

select '--- grouping sets ---' format TSVRaw;

SELECT
    c1,
    uniqExact(c2)
FROM test
GROUP BY 
    GROUPING SETS(
      (c1),
      ()
    )
ORDER BY c1 DESC;
