---
sidebar_position: 55
sidebar_label: s3Cluster
---

# s3Cluster Table Function {#s3Cluster-table-function}

Allows processing files from [Amazon S3](https://aws.amazon.com/s3/) in parallel from many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster, discloses asterics in S3 file path, and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

**Syntax**

``` sql
s3Cluster(cluster_name, source, [access_key_id, secret_access_key,] format, structure)
```

**Arguments**

-   `cluster_name` — Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.
-   `source` — URL to a file or a bunch of files. Supports following wildcards in readonly mode: `*`, `?`, `{'abc','def'}` and `{N..M}` where `N`, `M` — numbers, `abc`, `def` — strings. For more information see [Wildcards In Path](../../engines/table-engines/integrations/s3.md#wildcards-in-path).
-   `access_key_id` and `secret_access_key` — Keys that specify credentials to use with given endpoint. Optional.
-   `format` — The [format](../../interfaces/formats.md#formats) of the file.
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**Returned value**

A table with the specified structure for reading or writing data in the specified file.

**Examples**

Select the data from all files in the cluster `cluster_simple`:

``` sql
SELECT * FROM s3Cluster('cluster_simple', 'http://minio1:9001/root/data/{clickhouse,database}/*', 'minio', 'minio123', 'CSV', 'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))') ORDER BY (name, value, polygon);
```

Count the total amount of rows in all files in the cluster `cluster_simple`:

``` sql
SELECT count(*) FROM s3Cluster('cluster_simple', 'http://minio1:9001/root/data/{clickhouse,database}/*', 'minio', 'minio123', 'CSV', 'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))');
```

:::warning    
If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
:::

**See Also**

-   [S3 engine](../../engines/table-engines/integrations/s3.md)
-   [s3 table function](../../sql-reference/table-functions/s3.md)
