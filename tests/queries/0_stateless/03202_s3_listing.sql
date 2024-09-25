-- Tags: no-fasttest

-- A smoke test: checks that the listing still works with parallelization options.
SET remote_filesystem_read_method = 'read', max_download_threads=1;
SET s3_use_parallel_listing = 1, s3_parallel_listing_max_threads = 100, s3_parallel_listing_num_requests = 200, s3_parallel_listing_multiplication_ratio = 0.8;

SELECT count() FROM s3('https://clickhouse-public-datasets.s3.amazonaws.com/musicbrainz/mlhdplus-complete/0*', One)
