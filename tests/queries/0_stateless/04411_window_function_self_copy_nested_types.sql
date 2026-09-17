-- Nested / dynamically-typed results over a fixed frame.
SELECT DISTINCT toString(any(v) OVER ()) FROM (SELECT (42::UInt64)::Variant(UInt64, String) AS v FROM numbers(10000));
SELECT DISTINCT toString(any(d) OVER ()) FROM (SELECT (7::UInt64)::Dynamic AS d FROM numbers(10000));
SELECT DISTINCT toString(any(j) OVER ()) FROM (SELECT materialize('{"a":5}')::JSON AS j FROM numbers(10000));

-- Plain result types over a fixed frame still produce correct values.
SELECT DISTINCT sum(number) OVER () FROM numbers(10000);
SELECT DISTINCT max(toString(number % 7)) OVER () FROM numbers(10000);
SELECT DISTINCT max(if(number % 3 = 0, NULL, toNullable(number))) OVER () FROM numbers(10000);
SELECT DISTINCT quantilesExact(0.5, 0.9)(number) OVER () FROM numbers(10000);

-- The same fast path also fires inside a single RANGE peer group, not only for OVER ().
SELECT DISTINCT toString(any(v) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))
FROM (SELECT 0 AS k, (42::UInt64)::Variant(UInt64, String) AS v FROM numbers(10000));
