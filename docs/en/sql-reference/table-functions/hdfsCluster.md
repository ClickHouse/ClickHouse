---
sidebar_position: 55
sidebar_label: hdfsCluster
---

# hdfsCluster Table Function {#hdfsCluster-table-function}

Allows processing files from HDFS in parallel from many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster, discloses asterics in HDFS file path, and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

**Syntax**

``` sql
hdfsCluster(cluster_name, URI, format, structure)
```

**Arguments**

-   `cluster_name` — Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.
-   `URI` — URI to a file or a bunch of files. Supports following wildcards in readonly mode: `*`, `?`, `{'abc','def'}` and `{N..M}` where `N`, `M` — numbers, `abc`, `def` — strings. For more information see [Wildcards In Path](../../engines/table-engines/integrations/s3.md#wildcards-in-path).
-   `format` — The [format](../../interfaces/formats.md#formats) of the file.
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**Returned value**

A table with the specified structure for reading data in the specified file.

**Examples**

1.  Suppose that we have a ClickHouse cluster named `cluster_simple`, and several files with following URIs on HDFS:

-   ‘hdfs://hdfs1:9000/some_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_3’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_3’

2.  Query the amount of rows in these files:

``` sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3.  Query the amount of rows in all files of these two directories:

``` sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::warning    
If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
:::

**See Also**

-   [HDFS engine](../../engines/table-engines/integrations/hdfs.md)
-   [HDFS table function](../../sql-reference/table-functions/hdfs.md)
