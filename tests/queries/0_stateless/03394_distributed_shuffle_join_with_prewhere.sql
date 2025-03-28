CREATE TABLE test(path String, lang String, hits UInt64) ENGINE MergeTree() ORDER BY tuple();

INSERT INTO test SELECT 'path' || number::String, 'en', number FROM numbers(5);
INSERT INTO test SELECT 'path' || number::String, 'de', number FROM numbers(10);
INSERT INTO test SELECT 'path' || number::String, 'ua', number FROM numbers(15);
INSERT INTO test SELECT 'path' || number::String, 'jp', number FROM numbers(20);


SET
    make_distributed_plan = 1,
    enable_parallel_replicas = 0,
    default_shuffle_join_bucket_count=3,
    force_exchange_kind='Streaming';

SELECT count() FROM test AS en, test AS de WHERE (en.path = de.path) AND (en.lang = 'en') AND (de.lang = 'de');

EXPLAIN actions = 1 SELECT count() FROM test AS en, test AS de WHERE (en.path = de.path) AND (en.lang = 'en') AND (de.lang = 'de');
