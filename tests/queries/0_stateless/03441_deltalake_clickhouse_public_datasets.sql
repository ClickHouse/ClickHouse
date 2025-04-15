-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SELECT count()
FROM deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/', SETTINGS allow_experimental_delta_kernel_rs = 1);
