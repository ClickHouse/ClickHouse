---
description: 'This is an extension to the deltaLake table function.'
sidebar_label: 'deltaLakeCluster'
sidebar_position: 46
slug: /sql-reference/table-functions/deltalakeCluster
title: 'deltaLakeCluster'
---

# deltaLakeCluster Table Function

This is an extension to the [deltaLake](sql-reference/table-functions/deltalake.md) table function.

Allows processing files from [Delta Lake](https://github.com/delta-io/delta) tables in Amazon S3 in parallel from many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

**Syntax**

```sql
deltaLakeCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])
```

**Arguments**

- `cluster_name` — Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.

- Description of all other arguments coincides with description of arguments in equivalent [deltaLake](sql-reference/table-functions/deltalake.md) table function.

**Returned value**

A table with the specified structure for reading data from cluster in the specified Delta Lake table in S3.

**See Also**

- [deltaLake engine](engines/table-engines/integrations/deltalake.md)
- [deltaLake table function](sql-reference/table-functions/deltalake.md)
