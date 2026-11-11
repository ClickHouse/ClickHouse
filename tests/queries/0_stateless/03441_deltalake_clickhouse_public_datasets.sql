-- Tags: no-fasttest, no-msan
-- Tag no-fasttest: Depends on AWS
-- Tag no-msan: delta-kernel is not built with msan

SELECT count()
FROM deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/', NOSIGN, SETTINGS allow_experimental_delta_kernel_rs = 1);
