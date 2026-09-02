-- Tags: replica

SELECT arrayFilter(x -> materialize(0), materialize([0])) AS p, arrayAll(y -> arrayExists(x -> y != x, p), p) AS test;
SELECT arrayFilter(x -> materialize(0), materialize([''])) AS p, arrayAll(y -> arrayExists(x -> y != x, p), p) AS test;
