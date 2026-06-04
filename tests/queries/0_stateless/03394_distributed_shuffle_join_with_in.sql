-- Tags: no-old-analyzer

-- A nonzero max_rows_to_group_by keeps aggregation single-node, so pin it to 0.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS test;

CREATE TABLE test(path String, lang String, hits UInt64) ENGINE MergeTree() ORDER BY tuple();

INSERT INTO test SELECT 'path_' || number::String, 'en', number FROM numbers(5);
INSERT INTO test SELECT 'path_' || (number%3)::String, 'de', number%4 FROM numbers(10);

INSERT INTO test SELECT 'path_' || number::String, 'en', number FROM numbers(5);
INSERT INTO test SELECT 'path_' || (number%3)::String, 'de', number%4 FROM numbers(10);

SET query_plan_join_swap_table = 0;


SET
    optimize_move_to_prewhere = 1,
    query_plan_optimize_prewhere = 1,
    make_distributed_plan = 1,
    enable_parallel_replicas = 0,
    enable_join_runtime_filters = 1,
    distributed_plan_default_shuffle_join_bucket_count=3,
    distributed_plan_default_reader_bucket_count=3,
    distributed_plan_force_exchange_kind='Streaming',
    distributed_plan_optimize_exchanges = 1,
    distributed_plan_max_rows_to_broadcast=0;

SELECT '----------';

-- Query with col IN (val1, val2, ...)
-- It passes the set corresponding to IN conditions as ColumnSet
EXPLAIN SELECT *
FROM
   (SELECT path, sum(hits) as hits FROM test WHERE lang IN ('en', 'de') GROUP BY path) AS en,
   (SELECT path, sum(hits) as hits FROM test WHERE lang = 'de' GROUP BY path) AS de
WHERE (en.path = de.path)
ORDER BY ALL;

SELECT '----------';

SELECT *
FROM
   (SELECT path, sum(hits) as hits FROM test WHERE lang IN ('en', 'de') GROUP BY path) AS en,
   (SELECT path, sum(hits) as hits FROM test WHERE lang = 'de' GROUP BY path) AS de
WHERE (en.path = de.path)
ORDER BY ALL;

DROP TABLE test;
