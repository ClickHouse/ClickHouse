---
slug: /sql-reference/table-functions/hudiCluster
sidebar_position: 86
sidebar_label: hudiCluster
title: "hudiCluster Table Function"
---
This is an extension to the [hudi](/docs/sql-reference/table-functions/hudi.md) table function.

Allows processing files from Apache [Hudi](https://hudi.apache.org/) tables in Amazon S3 in parallel from many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

**Syntax**

``` sql
hudiCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])
```

**Arguments**

- `cluster_name` — Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.

- Description of all other arguments coincides with description of arguments in equivalent [hudi](/docs/sql-reference/table-functions/hudi.md) table function.

**Returned value**

A table with the specified structure for reading data from cluster in the specified Hudi table in S3.

**See Also**

- [Hudi engine](/docs/engines/table-engines/integrations/hudi.md)
- [Hudi table function](/docs/sql-reference/table-functions/hudi.md)
