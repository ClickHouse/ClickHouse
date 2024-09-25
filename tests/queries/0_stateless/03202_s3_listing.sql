-- Tags: no-fastest

-- A smoke test: checks that the listing still works with parallelization options.
SET s3_use_parallel_listing = 1;
SET s3_num_workers = 100;
SET s3_num_parallel_requests = 200;
SET s3_multiplication_length = 0.8;

SELECT count() FROM s3('https://clickhouse-public-datasets.s3.amazonaws.com/musicbrainz/mlhdplus-complete/0*', One)
