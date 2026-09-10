-- Tags: no-parallel-replicas

DROP TABLE IF EXISTS test_indexHint_prewhere;

CREATE TABLE test_indexHint_prewhere
(
    id UInt32,
    colA String,
    colB String,
    INDEX colA_tokens_idx tokens(colA) TYPE bloom_filter GRANULARITY 1,
    INDEX colB_tokens_idx tokens(colB) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO test_indexHint_prewhere SELECT number, randomPrintableASCII(30), randomPrintableASCII(40) FROM numbers(100);

SELECT count() FROM
(
    EXPLAIN actions = 1 SELECT * FROM test_indexHint_prewhere
    WHERE (id IN (62, 88, 89, 67)) AND ((colA LIKE '%ymo82%') OR (colB LIKE '%dKappNQY6I%'))
)
WHERE explain LIKE '%Prewhere filter column%colA%colB%';

SELECT count() FROM
(
    EXPLAIN actions = 1 SELECT * FROM test_indexHint_prewhere
    WHERE (id IN (62, 88, 89, 67)) AND ((indexHint(has(tokens(colA), 'ymo82')) AND (colA LIKE '%ymo82%')) OR (indexHint(has(tokens(colB), 'dKappNQY6I')) AND (colB LIKE '%dKappNQY6I%')))
)
WHERE explain LIKE '%Prewhere filter column%colA%colB%';

DROP TABLE test_indexHint_prewhere;
