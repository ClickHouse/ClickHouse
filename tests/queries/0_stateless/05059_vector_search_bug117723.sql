-- Tags: no-fasttest, no-parallel-replicas

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/117723
-- Vector search where `LIMIT` + `OFFSET` overflows `UInt64`

SET query_plan_try_use_vector_search = 1;

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, [toFloat32(number), toFloat32(number)] FROM numbers(12);

SELECT '-- ordinary LIMIT: index usage expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [0., 2.])
    LIMIT 3
)
WHERE explain LIKE '%vector_similarity%';

SELECT '-- overflowing LIMIT + OFFSET: index usage not expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [0., 2.])
    LIMIT 18446744073709551615 OFFSET 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT count() FROM (SELECT id FROM tab ORDER BY L2Distance(vec, [0., 2.]) LIMIT 18446744073709551615 OFFSET 1);

-- `_distance` only makes sense for vector search. Disallow read in case of overflow.
SELECT id, _distance FROM tab ORDER BY L2Distance(vec, [0., 2.]) LIMIT 18446744073709551615 OFFSET 1; -- { serverError ILLEGAL_COLUMN }

DROP TABLE tab;
