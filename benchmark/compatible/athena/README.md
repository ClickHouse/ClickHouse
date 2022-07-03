Data Sources -> AWS Data Catalog -> Create Table -> Use S3 bucket data

Note: Athena does not support files. Only directories:

```
aws s3 cp s3://clickhouse-public-datasets/hits_compatible/hits.parquet s3://clickhouse-public-datasets/hits_compatible/athena/hits.parquet 
```

Go to query editor and run `create.sql`.
