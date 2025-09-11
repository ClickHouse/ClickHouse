set max_threads=4;

create table test(c1 Int64, c2 Int64)
  Engine=MergeTree order by c2
as  
SELECT
    1071106,
    5+abs(1 + 151703 * (sqrt(-2 * log(u1)) * cos(2 * pi() * u2)))
FROM
(
    SELECT
        (cityHash64(number) % 1000000) / 1000000.0 AS u1,
        (cityHash64(number+12345) % 1000000) / 1000000.0 AS u2
    FROM numbers(1e6)
) 
union all
SELECT
    1071102,
    8+abs(1 + 151693 * (sqrt(-2 * log(u1)) * cos(2 * pi() * u2)))
FROM
(
    SELECT
        (cityHash64(number) % 1000000) / 1000000.0 AS u1,
        (cityHash64(number+12345) % 1000000) / 1000000.0 AS u2
    FROM numbers(1e6)
);

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
