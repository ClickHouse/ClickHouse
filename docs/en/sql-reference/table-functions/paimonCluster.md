---
description: 'An extension to the paimon table function which allows processing files
  from Apache Paimon in parallel from many nodes in a specified cluster.'
sidebar_label: 'paimonCluster'
sidebar_position: 91
slug: /sql-reference/table-functions/paimonCluster
title: 'paimonCluster'
doc_type: 'reference'
---

# paimonCluster Table Function

This is an extension to the [paimon](/sql-reference/table-functions/paimon.md) table function.

Allows processing files from Apache [Paimon](https://paimon.apache.org/) in parallel from many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

## Syntax {#syntax}

```sql
paimonS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])

paimonAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

paimonHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
```

## Arguments {#arguments}

- `cluster_name` — Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.
- Description of all other arguments coincides with description of arguments in equivalent [paimon](/sql-reference/table-functions/paimon.md) table function.

**Returned value**

A table with the specified structure for reading data from cluster in the specified Paimon table.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

**See Also**

- [Paimon table function](sql-reference/table-functions/paimon.md)
