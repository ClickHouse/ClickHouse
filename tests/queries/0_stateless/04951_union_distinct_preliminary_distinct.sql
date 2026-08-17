-- UNION DISTINCT gets a preliminary per-stream DISTINCT, so it plans like its
-- SELECT DISTINCT over UNION ALL rewrite. INTERSECT/EXCEPT DISTINCT already emit a
-- single stream, so they keep the single final DISTINCT.

SET enable_analyzer = 1;

SELECT '-- analyzer: UNION DISTINCT has a preliminary DISTINCT';
SELECT count() > 0 FROM (
    EXPLAIN PLAN SELECT 1 AS x UNION DISTINCT SELECT 2 AS x
) WHERE explain ILIKE '%Preliminary DISTINCT%';

SELECT '-- analyzer: the rewrite plans the same way';
SELECT count() > 0 FROM (
    EXPLAIN PLAN SELECT DISTINCT * FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x)
) WHERE explain ILIKE '%Preliminary DISTINCT%';

SELECT '-- analyzer: INTERSECT/EXCEPT DISTINCT keep a single DISTINCT';
SELECT count() FROM (
    EXPLAIN PLAN SELECT 1 AS x INTERSECT DISTINCT SELECT 1 AS x
) WHERE explain ILIKE '%Preliminary%';
SELECT count() FROM (
    EXPLAIN PLAN SELECT 1 AS x EXCEPT DISTINCT SELECT 2 AS x
) WHERE explain ILIKE '%Preliminary%';

SET enable_analyzer = 0;

SELECT '-- old analyzer: UNION DISTINCT has a preliminary DISTINCT';
SELECT count() > 0 FROM (
    EXPLAIN PLAN SELECT 1 AS x UNION DISTINCT SELECT 2 AS x
) WHERE explain ILIKE '%Preliminary DISTINCT%';

SET enable_analyzer = 1;

SELECT '-- results match the rewrite, per type';
CREATE TEMPORARY TABLE src (i UInt64);
INSERT INTO src SELECT number FROM numbers(600);

SELECT count(), sum(cityHash64(x)) FROM (
    (SELECT i % 200 AS x FROM src) UNION DISTINCT (SELECT i % 300 AS x FROM src)
);
SELECT count(), sum(cityHash64(x)) FROM (SELECT DISTINCT * FROM (
    (SELECT i % 200 AS x FROM src) UNION ALL (SELECT i % 300 AS x FROM src)
));

SELECT count(), sum(cityHash64(x)) FROM (
    (SELECT if(i % 7 = 0, NULL, i % 200)::Nullable(UInt64) AS x FROM src)
    UNION DISTINCT (SELECT if(i % 5 = 0, NULL, i % 300)::Nullable(UInt64) AS x FROM src)
);
SELECT count(), sum(cityHash64(x)) FROM (SELECT DISTINCT * FROM (
    (SELECT if(i % 7 = 0, NULL, i % 200)::Nullable(UInt64) AS x FROM src)
    UNION ALL (SELECT if(i % 5 = 0, NULL, i % 300)::Nullable(UInt64) AS x FROM src)
));

SELECT count(), sum(cityHash64(x)) FROM (
    (SELECT toLowCardinality(toString(i % 200)) AS x FROM src)
    UNION DISTINCT (SELECT toLowCardinality(toString(i % 300)) AS x FROM src)
);
SELECT count(), sum(cityHash64(x)) FROM (SELECT DISTINCT * FROM (
    (SELECT toLowCardinality(toString(i % 200)) AS x FROM src)
    UNION ALL (SELECT toLowCardinality(toString(i % 300)) AS x FROM src)
));

SELECT count(), sum(cityHash64(x)) FROM (
    (SELECT [i % 20, i % 10] AS x FROM src) UNION DISTINCT (SELECT [i % 30, i % 10] AS x FROM src)
);
SELECT count(), sum(cityHash64(x)) FROM (SELECT DISTINCT * FROM (
    (SELECT [i % 20, i % 10] AS x FROM src) UNION ALL (SELECT [i % 30, i % 10] AS x FROM src)
));

SELECT '-- branch types are coerced, not compared raw';
SELECT count(), sum(cityHash64(x)) FROM (SELECT 1::UInt8 AS x UNION DISTINCT SELECT -5::Int64 AS x);

SELECT '-- a branch header that diverges structurally still deduplicates';
SELECT x FROM (SELECT 1 AS x UNION DISTINCT SELECT materialize(1) AS x);
SELECT count() FROM (
    SELECT sumState(i) AS s FROM src UNION DISTINCT SELECT sumState(number) AS s FROM numbers(10)
);

SELECT '-- chains and nesting';
SELECT count(), sum(x) FROM (
    (SELECT i % 200 AS x FROM src) UNION DISTINCT (SELECT i % 300 AS x FROM src)
    UNION DISTINCT (SELECT i % 400 AS x FROM src)
);
SELECT count(), sum(x) FROM (
    (SELECT i % 200 AS x FROM src) UNION DISTINCT (SELECT i % 300 AS x FROM src)
    UNION ALL (SELECT i % 400 AS x FROM src)
);
SELECT count(), sum(x) FROM (
    ((SELECT i % 200 AS x FROM src) UNION ALL (SELECT i % 300 AS x FROM src))
    UNION DISTINCT (SELECT i % 400 AS x FROM src)
);

SELECT '-- a bare UNION under union_default_mode takes the same path';
SELECT count(), sum(x) FROM (
    (SELECT i % 200 AS x FROM src) UNION (SELECT i % 300 AS x FROM src)
) SETTINGS union_default_mode = 'DISTINCT';

SELECT '-- LIMIT does not truncate the preliminary phase';
SELECT count() FROM (
    SELECT x FROM ((SELECT i % 200 AS x FROM src) UNION DISTINCT (SELECT i % 300 AS x FROM src)) LIMIT 20
);
SELECT groupArray(x) FROM (
    SELECT x FROM ((SELECT i % 200 AS x FROM src) UNION DISTINCT (SELECT i % 300 AS x FROM src))
    ORDER BY x LIMIT 5
);
SELECT count() FROM (
    SELECT x, x % 7 AS g FROM ((SELECT i % 200 AS x FROM src) UNION DISTINCT (SELECT i % 300 AS x FROM src))
    LIMIT 2 BY g
);

SELECT '-- size limits are enforced the same way as in the rewrite';
SELECT count() FROM (
    (SELECT i AS x FROM src) UNION DISTINCT (SELECT i AS x FROM src)
) SETTINGS max_rows_in_distinct = 100, distinct_overflow_mode = 'break', max_block_size = 50;
SELECT count() FROM (SELECT DISTINCT * FROM (
    (SELECT i AS x FROM src) UNION ALL (SELECT i AS x FROM src)
)) SETTINGS max_rows_in_distinct = 100, distinct_overflow_mode = 'break', max_block_size = 50;

SELECT count() FROM (
    (SELECT i AS x FROM src) UNION DISTINCT (SELECT i AS x FROM src)
) SETTINGS max_rows_in_distinct = 100, distinct_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
