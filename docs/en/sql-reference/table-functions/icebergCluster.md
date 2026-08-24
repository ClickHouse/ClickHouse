---
description: 'An extension to the iceberg table function which allows processing files
  from Apache Iceberg in parallel from many nodes in a specified cluster.'
sidebar_label: 'icebergCluster'
sidebar_position: 91
slug: /sql-reference/table-functions/icebergCluster
title: 'icebergCluster'
doc_type: 'reference'
---

This is an extension to the [iceberg](/sql-reference/table-functions/iceberg.md) table function.

Allows processing files from Apache [Iceberg](https://iceberg.apache.org/) in parallel from many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

## Deletion vectors on cluster reads {#deletion-vectors-cluster}

Iceberg v3 [deletion vectors](https://iceberg.apache.org/spec/#deletion-vectors) are loaded on the **initiator** while it distributes tasks: for each data file the initiator reads the Puffin blob, validates it, materializes deleted row positions, and attaches the bitmap to the task sent to workers. Workers apply that bitmap when reading Parquet; they do not fetch or parse the Puffin file again for that path. See [Processing of tables with deleted rows](/sql-reference/table-functions/iceberg.md#deleted-rows) for format limits and caching.

On wide tables with many deletion vectors, initiator-side decode and per-task bitmap serialization can become a bottleneck even when Parquet reads are well parallelized across the cluster.

## Syntax {#syntax}

```sql
icebergS3Cluster(cluster_name, url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3Cluster(cluster_name, named_collection[, option=value [,..]])

icebergAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzureCluster(cluster_name, named_collection[, option=value [,..]])

icebergHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
icebergHDFSCluster(cluster_name, named_collection[, option=value [,..]])
```

## Arguments {#arguments}

- `cluster_name` — Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.
- Description of all other arguments coincides with description of arguments in equivalent [iceberg](/sql-reference/table-functions/iceberg.md) table function.
- An optional `extra_credentials` parameter can be used to pass a `role_arn` for role-based access in ClickHouse Cloud. See [Secure S3](/cloud/data-sources/secure-s3) for configuration steps.

**Returned value**

A table with the specified structure for reading data from cluster in the specified Iceberg table.

**Examples**

```sql
SELECT * FROM icebergS3Cluster('cluster_simple', 'http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## Altinity Antalya branch

### `icebergLocalCluster` table function

Only in the Altinity Antalya branch, `icebergLocalCluster` designed to make distributed cluster queries when Iceberg data is stored on shared network storage mounted with a local path. The path must be identical on all replicas.

```sql
icebergLocalCluster(cluster_name, path_to_table, [,format] [,compression_method])
```

### Specify storage type in function arguments

Only in the Altinity Antalya branch, the `icebergCluster` table function supports all storage backends. The storage backend can be specified using the named argument `storage_type`. Valid values include `s3`, `azure`, `hdfs`, and `local`.

```sql
icebergCluster(storage_type='s3', cluster_name, url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method])

icebergCluster(storage_type='azure', cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

icebergCluster(storage_type='hdfs', cluster_name, path_to_table, [,format] [,compression_method])

icebergCluster(storage_type='local', cluster_name, path_to_table, [,format] [,compression_method])
```

### Specify storage type in a named collection

Only in the Altinity Antalya branch, `storage_type` can be part of a named collection.

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
            <storage_type>s3</storage_type>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
icebergCluster(iceberg_conf[, option=value [,..]])
```

The default value for `storage_type` is `s3`.

### `object_storage_cluster` setting.

Only in the Altinity Antalya branch, an alternative syntax for `icebergCluster` table function is available. This allows the `iceberg` function to be used with the non-empty `object_storage_cluster` setting, specifying a cluster name. This enables distributed queries over Iceberg table across a ClickHouse cluster.

```sql
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method]) SETTINGS object_storage_cluster='cluster_name'

icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method]) SETTINGS object_storage_cluster='cluster_name'

icebergHDFS(path_to_table, [,format] [,compression_method]) SETTINGS object_storage_cluster='cluster_name'

icebergLocal(path_to_table, [,format] [,compression_method]) SETTINGS object_storage_cluster='cluster_name'

icebergS3(option=value [,..]) SETTINGS object_storage_cluster='cluster_name'

iceberg(storage_type='s3', url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method]) SETTINGS object_storage_cluster='cluster_name'

iceberg(storage_type='azure', connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method]) SETTINGS object_storage_cluster='cluster_name'

iceberg(storage_type='hdfs', path_to_table, [,format] [,compression_method]) SETTINGS object_storage_cluster='cluster_name'

iceberg(storage_type='local', path_to_table, [,format] [,compression_method]) SETTINGS object_storage_cluster='cluster_name'

iceberg(iceberg_conf[, option=value [,..]]) SETTINGS object_storage_cluster='cluster_name'
```

**See Also**

- [Iceberg engine](/engines/table-engines/integrations/iceberg.md)
- [Iceberg table function](/sql-reference/table-functions/iceberg.md)
