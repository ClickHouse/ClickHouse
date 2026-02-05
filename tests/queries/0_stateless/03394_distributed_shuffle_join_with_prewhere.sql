SET distributed_plan_optimize_exchanges = 1;

CREATE TABLE test(path String, lang String, hits UInt64) ENGINE MergeTree()
ORDER BY tuple()
SETTINGS auto_statistics_types = 'tdigest,countmin,uniq,minmax';

INSERT INTO test SELECT 'path' || number::String, 'en', number FROM numbers(5);
INSERT INTO test SELECT 'path' || number::String, 'de', number FROM numbers(10);
INSERT INTO test SELECT 'path' || number::String, 'ua', number FROM numbers(15);
INSERT INTO test SELECT 'path' || number::String, 'jp', number FROM numbers(20);

SET query_plan_join_swap_table = 0;

SET
    make_distributed_plan = 1,
    enable_parallel_replicas = 0,
    distributed_plan_default_shuffle_join_bucket_count=3,
    distributed_plan_default_reader_bucket_count=3,
    distributed_plan_force_exchange_kind='Streaming';

SET enable_join_runtime_filters=1;

SELECT count() FROM test AS en, test AS de WHERE (en.path = de.path) AND (en.lang = 'en') AND (de.lang = 'de');

SELECT REGEXP_REPLACE(explain, '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID') AS explain FROM (
    EXPLAIN actions = 1 SELECT count() FROM test AS en, test AS de WHERE (en.path = de.path) AND (en.lang = 'en') AND (de.lang = 'de')
) WHERE
    explain LIKE '%Join%' OR explain LIKE '%ReadFrom%' OR explain LIKE '%Aggregating%' OR explain LIKE '%Merging%' OR explain LIKE '%filter column%'
    OR explain LIKE '%Shuffle%' OR explain LIKE '%Broadcast%' OR explain LIKE '%Scatter%' OR explain LIKE '%Gather%';
