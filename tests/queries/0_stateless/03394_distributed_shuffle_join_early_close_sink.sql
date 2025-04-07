# Tags: long, no-fasttest

CREATE TABLE test(id UInt64, data String) ENGINE=MergeTree() ORDER BY id SETTINGS index_granularity=10000;

INSERT INTO test SELECT number, '' FROM numbers(10000000);

-- Joining on a.id%1 so that only one bucket is not empty
-- After building hash table for an empty bucket the pipeline will stop reading from probing input and will close Exchange stream before reading all the data from it
SELECT count()
FROM test AS b JOIN test AS a ON a.id%1 = b.id%2
WHERE a.id < 10 AND b.id < 10000000 AND NOT sleepEachRow(0.000001)
SETTINGS make_distributed_plan=1, enable_parallel_replicas=0, default_shuffle_join_bucket_count=4, default_reader_bucket_count=5;

