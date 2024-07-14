SET query_profiler_cpu_time_period_ns=10000000;
SET s3_use_parallel_listing = 1;
SET s3_num_workers = 100;
SET s3_num_parallel_requests = 200;
SET s3_multiplication_length = 0.8;

SELECT count() FROM s3('https://clickhouse-public-datasets.s3.amazonaws.com/musicbrainz/mlhdplus-complete/0*', One)
