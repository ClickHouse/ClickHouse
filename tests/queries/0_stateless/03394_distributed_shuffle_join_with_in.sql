-- Tags: no-old-analyzer

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET explain_query_plan_default = 'legacy';
-- Distributed aggregation cannot enforce a global `max_rows_to_group_by`, so pin it to 0.
SET max_rows_to_group_by = 0;
-- Pin off: statistics change the estimated group count, flipping the distributed aggregation
-- strategy (Shuffle vs partial+merge) and thus the asserted plan.
SET use_statistics = 0;

DROP TABLE IF EXISTS test;

-- auto_statistics_types='' pins out randomized column statistics: with them, the row-count
-- estimator flips the distributed join from Shuffle to Scatter and changes the plan shape.
CREATE TABLE test(path String, lang String, hits UInt64) ENGINE MergeTree() ORDER BY tuple() SETTINGS auto_statistics_types='';

INSERT INTO test SELECT 'path_' || number::String, 'en', number FROM numbers(5);
INSERT INTO test SELECT 'path_' || (number%3)::String, 'de', number%4 FROM numbers(10);

INSERT INTO test SELECT 'path_' || number::String, 'en', number FROM numbers(5);
INSERT INTO test SELECT 'path_' || (number%3)::String, 'de', number%4 FROM numbers(10);

SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;


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
