-- Tags: no-fasttest, no-msan, no-parallel, no-parallel-replicas, no-random-settings
-- Tag no-fasttest: Depends on AWS
-- Tag no-msan: delta-kernel is not built with msan
-- Tag no-parallel, no-parallel-replicas: the cache is system-wide so concurrent queries can influence the cache

set log_queries = 1;
system drop parquet metadata cache;

select count(), ParamCurrency
from deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/', nosign, settings allow_experimental_delta_kernel_rs = 1) 
where Robotness > 0
group by ParamCurrency
format null 
settings use_parquet_metadata_cache=1, input_format_parquet_use_native_reader_v3=1, log_comment='04103-pq-cache-miss-and-load';

select count(), ParamCurrency
from deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/', nosign, settings allow_experimental_delta_kernel_rs = 1)
where Robotness > 0
group by ParamCurrency
format null
settings use_parquet_metadata_cache=1, input_format_parquet_use_native_reader_v3=1, log_comment='04103-pq-cache-hit';

system flush logs query_log;

select ProfileEvents['ParquetMetadataCacheHits'], ProfileEvents['ParquetMetadataCacheMisses']
from system.query_log
where (log_comment = '04103-pq-cache-miss-and-load') and (type = 'QueryFinish') and current_database = currentDatabase();

select ProfileEvents['ParquetMetadataCacheHits'], ProfileEvents['ParquetMetadataCacheMisses']
from system.query_log
where (log_comment = '04103-pq-cache-hit') and (type = 'QueryFinish') and current_database = currentDatabase();
