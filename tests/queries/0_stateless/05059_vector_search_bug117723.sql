-- Tags: no-fasttest
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/117723
-- A `LIMIT` + `OFFSET` that overflows `UInt64` must not be read as a request for zero neighbours.

SET parallel_replicas_local_plan = 1;

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

SELECT count() FROM (SELECT id FROM tab ORDER BY L2Distance(vec, [0., 2.]) LIMIT 18446744073709551615 OFFSET 1);

DROP TABLE tab;
