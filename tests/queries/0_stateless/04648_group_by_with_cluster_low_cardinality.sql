-- `LowCardinality(...)` is only a storage wrapper over the logical key type, so a
-- `LowCardinality(String)` / `LowCardinality(UInt64)` column must be usable as a
-- `WITH CLUSTER` key exactly like its unwrapped counterpart, and the result must keep the
-- `LowCardinality` type.
-- See https://github.com/ClickHouse/ClickHouse/pull/101878

SET enable_analyzer = 1; -- `WITH CLUSTER` is implemented for the analyzer only
SET allow_experimental_group_by_with_cluster = 1;

-- 1D `LowCardinality(String)`: `apple` and `apples` are one edit apart, `banana` is far away.
SELECT 'lc string';
SELECT s, count() AS c
FROM VALUES('s LowCardinality(String)', ('apple'), ('apples'), ('apple'), ('banana'))
GROUP BY s WITH CLUSTER 1
ORDER BY s;

-- The clustered key keeps its `LowCardinality` type.
SELECT 'lc string type';
SELECT DISTINCT toTypeName(s)
FROM VALUES('s LowCardinality(String)', ('apple'), ('apples'))
GROUP BY s WITH CLUSTER 1;

-- The same data without the wrapper must cluster identically.
SELECT 'string reference';
SELECT s, count() AS c
FROM VALUES('s String', ('apple'), ('apples'), ('apple'), ('banana'))
GROUP BY s WITH CLUSTER 1
ORDER BY s;

-- 1D `LowCardinality(UInt64)`: 1 and 2 merge, 10 stays alone.
SELECT 'lc uint64';
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT x, count() AS c
    FROM (
        SELECT toLowCardinality(toUInt64(1)) AS x
        UNION ALL SELECT toLowCardinality(toUInt64(2))
        UNION ALL SELECT toLowCardinality(toUInt64(10))
    )
    GROUP BY x WITH CLUSTER 1
);

-- The clustered numeric key keeps its `LowCardinality` type too.
SELECT 'lc uint64 type';
SELECT DISTINCT toTypeName(x)
FROM (SELECT toLowCardinality(toUInt64(number)) AS x FROM numbers(3))
GROUP BY x WITH CLUSTER 1;

-- 1D `LowCardinality(Float64)`.
SELECT 'lc float64';
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT x, count() AS c
    FROM (
        SELECT toLowCardinality(toFloat64(1.0)) AS x
        UNION ALL SELECT toLowCardinality(toFloat64(1.25))
        UNION ALL SELECT toLowCardinality(toFloat64(5.0))
    )
    GROUP BY x WITH CLUSTER 0.5
);

-- 2D: both coordinates are `LowCardinality(Int32)`; `(0,0)` and `(1,1)` are within distance 2.
SELECT 'lc 2d';
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT x, y, count() AS c
    FROM (
        SELECT toLowCardinality(toInt32(0)) AS x, toLowCardinality(toInt32(0)) AS y
        UNION ALL SELECT toLowCardinality(toInt32(1)), toLowCardinality(toInt32(1))
        UNION ALL SELECT toLowCardinality(toInt32(100)), toLowCardinality(toInt32(100))
    )
    GROUP BY (x, y) WITH CLUSTER 2
);

-- `WITH CLUSTER 0` (exact match) with a wide `LowCardinality(UInt64)` range: the exact-match
-- path reads the key bits directly, so the values must stay in separate clusters.
SELECT 'lc uint64 zero distance wide';
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT x, count() AS c
    FROM (
        SELECT toLowCardinality(toUInt64(0)) AS x
        UNION ALL SELECT toLowCardinality(toUInt64(1152921504606846976))
    )
    GROUP BY x WITH CLUSTER 0
);

-- `LowCardinality(Nullable(String))` is still unsupported: only the wrapper is looked through,
-- `Nullable` keys remain rejected.
SELECT 'lc nullable rejected';
SELECT s FROM VALUES('s LowCardinality(Nullable(String))', ('a'), ('b'))
GROUP BY s WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }
