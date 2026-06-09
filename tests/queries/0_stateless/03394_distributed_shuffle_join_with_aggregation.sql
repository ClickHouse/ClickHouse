-- Tags: no-old-analyzer

-- Reset the global max_rows_to_group_by; distributed aggregation rejects a nonzero limit.
SET max_rows_to_group_by = 0;
SET distributed_plan_optimize_exchanges = 1;

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
    enable_join_runtime_filters=1,
    distributed_plan_default_shuffle_join_bucket_count=3,
    distributed_plan_default_reader_bucket_count=3,
    distributed_plan_force_exchange_kind='Streaming',
    distributed_plan_max_rows_to_broadcast=0;

SELECT '----------';

EXPLAIN SELECT *
FROM
   (SELECT path, sum(hits) as hits FROM test WHERE lang = 'en' GROUP BY path) AS en,
   (SELECT path, sum(hits) as hits FROM test WHERE lang = 'de' GROUP BY path) AS de
WHERE (en.path = de.path)
ORDER BY ALL
SETTINGS distributed_plan_optimize_exchanges=0;


SELECT '----------';

EXPLAIN SELECT *
FROM
   (SELECT path, sum(hits) as hits FROM test WHERE lang = 'en' GROUP BY path) AS en,
   (SELECT path, sum(hits) as hits FROM test WHERE lang = 'de' GROUP BY path) AS de
WHERE (en.path = de.path)
ORDER BY ALL;


SELECT '----------';

SELECT *
FROM
   (SELECT path, sum(hits) as hits FROM test WHERE lang = 'en' GROUP BY path) AS en,
   (SELECT path, sum(hits) as hits FROM test WHERE lang = 'de' GROUP BY path) AS de
WHERE (en.path = de.path)
ORDER BY ALL;


SELECT '----------';

SELECT en.path, count(), sum(en.hits), sum(de.hits)
FROM
   (SELECT * FROM test WHERE lang = 'en') AS en,
   (SELECT * FROM test WHERE lang = 'de') AS de
WHERE (en.path = de.path)
GROUP BY en.path
ORDER BY ALL;
