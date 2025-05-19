SET distributed_plan_optimize_exchanges = 1;
CREATE TABLE test(path String, lang String, hits UInt64) ENGINE MergeTree() ORDER BY tuple();

INSERT INTO test SELECT 'path_' || number::String, 'en', number FROM numbers(5);
INSERT INTO test SELECT 'path_' || (number%3)::String, 'de', number%4 FROM numbers(10);

INSERT INTO test SELECT 'path_' || number::String, 'en', number FROM numbers(5);
INSERT INTO test SELECT 'path_' || (number%3)::String, 'de', number%4 FROM numbers(10);


SET
    make_distributed_plan = 1,
    enable_parallel_replicas = 0,
    distributed_plan_default_shuffle_join_bucket_count=3,
    distributed_plan_default_reader_bucket_count=3,
    distributed_plan_force_exchange_kind='Streaming';

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
