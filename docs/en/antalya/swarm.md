# Antalya branch

## Swarm

### Difference with upstream version

#### `storage_type` argument in object storage functions

In upstream ClickHouse, there are several table functions to read Iceberg tables from different storage backends such as `icebergLocal`, `icebergS3`, `icebergAzure`, `icebergHDFS`, cluster variants, the `iceberg` function as a synonym for `icebergS3`, and table engines like `IcebergLocal`, `IcebergS3`, `IcebergAzure`, `IcebergHDFS`.

In the Antalya branch, the `iceberg` table function and the `Iceberg` table engine unify all variants into one by using a new named argument, `storage_type`, which can be one of `local`, `s3`, `azure`, or `hdfs`.

Old syntax examples:

```sql
SELECT * FROM icebergS3('http://minio1:9000/root/table_data', 'minio', 'minio123', 'Parquet');
SELECT * FROM icebergAzureCluster('mycluster', 'http://azurite1:30000/devstoreaccount1', 'cont', '/table_data', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'Parquet');
CREATE TABLE mytable ENGINE=IcebergHDFS('/table_data', 'Parquet');
```

New syntax examples:

```sql
SELECT * FROM iceberg(storage_type='s3', 'http://minio1:9000/root/table_data', 'minio', 'minio123', 'Parquet');
SELECT * FROM icebergCluster('mycluster', storage_type='azure', 'http://azurite1:30000/devstoreaccount1', 'cont', '/table_data', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'Parquet');
CREATE TABLE mytable ENGINE=Iceberg('/table_data', 'Parquet', storage_type='hdfs');
```

Also, if a named collection is used to store access parameters, the field `storage_type` can be included in the same named collection:

```xml
<named_collections>
    <s3>
        <url>http://minio1:9001/root/</url>
        <access_key_id>minio</access_key_id>
        <secret_access_key>minio123</secret_access_key>
        <storage_type>s3</storage_type>
    </s3>
</named_collections>
```

```sql
SELECT * FROM iceberg(s3, filename='table_data');
```

By default `storage_type` is `'s3'` to maintain backward compatibility.


#### `object_storage_cluster` setting

The new setting `object_storage_cluster` controls whether a single-node or cluster variant of table functions reading from object storage (e.g., `s3`, `azure`, `iceberg`, and their cluster variants like `s3Cluster`, `azureCluster`, `icebergCluster`) is used.

Old syntax examples:

```sql
SELECT * from s3Cluster('myCluster', 'http://minio1:9001/root/data/{clickhouse,database}/*', 'minio', 'minio123', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))');
SELECT * FROM icebergAzureCluster('mycluster', 'http://azurite1:30000/devstoreaccount1', 'cont', '/table_data', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'Parquet');
```

New syntax examples:

```sql
SELECT * from s3('http://minio1:9001/root/data/{clickhouse,database}/*', 'minio', 'minio123', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
        SETTINGS object_storage_cluster='myCluster';
SELECT * FROM icebergAzure('http://azurite1:30000/devstoreaccount1', 'cont', '/table_data', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'Parquet')
        SETTINGS object_storage_cluster='myCluster';
```

This setting also applies to table engines and can be used with tables managed by Iceberg Catalog.

Note: The upstream ClickHouse has introduced analogous settings, such as `parallel_replicas_for_cluster_engines` and `cluster_for_parallel_replicas`. Since version 25.10, these settings work with table engines. It is possible that in the future, the `object_storage_cluster` setting will be deprecated.
