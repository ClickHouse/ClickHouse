-- Exercise SingleValueDataGenericWithColumn in AggregateFunctions/SingleValueData.cpp
-- by running min / max / argMin / argMax over complex types (Tuple, Array, Map,
-- Decimal, IPv4, IPv6) and through the merge path (GROUP BY).
-- any() / anyLast() are intentionally excluded: they return an unspecified row
-- under parallelism and cannot be asserted on in a reference test.

SELECT '--- min / max of Tuple ---';
SELECT min(val), max(val) FROM (
    SELECT (1, 'a') AS val UNION ALL SELECT (0, 'b') UNION ALL SELECT (2, 'c'));

SELECT '--- min / max of Array ---';
SELECT min(val), max(val) FROM (
    SELECT [1, 2]::Array(Int32) AS val
    UNION ALL SELECT [3, 4]::Array(Int32)
    UNION ALL SELECT [0, 5]::Array(Int32));

SELECT '--- min / max of Map ---';
SELECT min(m), max(m) FROM (
    SELECT map('a', 1, 'b', 2)::Map(String, Int32) AS m
    UNION ALL SELECT map('a', 2, 'b', 3)::Map(String, Int32)
    UNION ALL SELECT map('a', 0, 'b', 5)::Map(String, Int32));

SELECT '--- min / max of Decimal ---';
SELECT min(x), max(x) FROM (
    SELECT CAST(1.5 AS Decimal(10, 2)) AS x
    UNION ALL SELECT CAST(3.2 AS Decimal(10, 2))
    UNION ALL SELECT CAST(-0.5 AS Decimal(10, 2)));

SELECT '--- min / max of IPv4 / IPv6 ---';
SELECT min(x), max(x) FROM (
    SELECT toIPv4('10.0.0.1') AS x
    UNION ALL SELECT toIPv4('192.168.1.1')
    UNION ALL SELECT toIPv4('1.1.1.1'));
SELECT min(x), max(x) FROM (
    SELECT toIPv6('::1') AS x
    UNION ALL SELECT toIPv6('::2')
    UNION ALL SELECT toIPv6('2001:db8::1'));

SELECT '--- argMin / argMax on Tuple value, numeric key ---';
SELECT argMin(val, key) FROM (
    SELECT (1, 'a') AS val, 10 AS key
    UNION ALL SELECT (2, 'b'), 5
    UNION ALL SELECT (3, 'c'), 20);
SELECT argMax(val, key) FROM (
    SELECT (1, 'a') AS val, 10 AS key
    UNION ALL SELECT (2, 'b'), 5
    UNION ALL SELECT (3, 'c'), 20);

SELECT '--- argMin / argMax on Array value ---';
SELECT argMin(val, key) FROM (
    SELECT [1, 2]::Array(Int32) AS val, 10 AS key
    UNION ALL SELECT [3, 4]::Array(Int32), 5);
SELECT argMax(val, key) FROM (
    SELECT [1, 2]::Array(Int32) AS val, 10 AS key
    UNION ALL SELECT [3, 4]::Array(Int32), 20);

SELECT '--- merge via GROUP BY (multi-partition) ---';
SELECT p, argMin(val, key), argMax(val, key), min(val), max(val) FROM (
    SELECT 1 AS p, (1, 'a') AS val, 10 AS key
    UNION ALL SELECT 1, (2, 'b'), 5
    UNION ALL SELECT 2, (3, 'c'), 8
    UNION ALL SELECT 2, (4, 'd'), 1)
GROUP BY p ORDER BY p;

SELECT '--- empty input on Tuple aggregates ---';
-- min/max/argMin/argMax over empty input use SingleValueDataGeneric's "unset"
-- path. The current behaviour returns a default-constructed tuple value rather
-- than NULL; this is asserted to detect regressions.
SELECT min(val), max(val), argMin(val, 1), argMax(val, 1)
FROM (SELECT (1, 'a') AS val WHERE 0);

SELECT '--- Nullable(Int32) aggregates (min/max only; any/anyLast are non-deterministic) ---';
SELECT min(x), max(x), count(x), countIf(x IS NULL) FROM (
    SELECT CAST(3 AS Nullable(Int32)) AS x
    UNION ALL SELECT NULL
    UNION ALL SELECT CAST(1 AS Nullable(Int32)));

SELECT '--- min/max over LowCardinality String ---';
SELECT min(x), max(x) FROM (
    SELECT CAST('hello', 'LowCardinality(String)') AS x
    UNION ALL SELECT CAST('apple', 'LowCardinality(String)')
    UNION ALL SELECT CAST('zebra', 'LowCardinality(String)'));

SELECT '--- min over nested Array(Tuple) ---';
SELECT min(val), max(val) FROM (
    SELECT [(1, 'a')]::Array(Tuple(Int32, String)) AS val
    UNION ALL SELECT [(2, 'b')]::Array(Tuple(Int32, String))
    UNION ALL SELECT [(0, 'c')]::Array(Tuple(Int32, String)));
