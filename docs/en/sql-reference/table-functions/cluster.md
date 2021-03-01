---
toc_priority: 50
toc_title: cluster
---

# cluster, clusterAllReplicas {#cluster-clusterallreplicas}

Allows to access all shards in an existing cluster which configured in `remote_servers` section without creating a [Distributed](../../engines/table-engines/special/distributed.md) table. One replica of each shard is queried.
`clusterAllReplicas` - same as `cluster` but all replicas are queried. Each replica in a cluster is used as separate shard/connection.

!!! note "Note"
    All available clusters are listed in the `system.clusters` table.

Signatures:

``` sql
cluster('cluster_name', db.table)
cluster('cluster_name', db, table)
clusterAllReplicas('cluster_name', db.table)
clusterAllReplicas('cluster_name', db, table)
```

`cluster_name` – Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.

Using the `cluster` and `clusterAllReplicas` table functions are less efficient than creating a `Distributed` table because in this case, the server connection is re-established for every request. When processing a large number of queries, please always create the `Distributed` table ahead of time, and don’t use the `cluster` and `clusterAllReplicas` table functions.

The `cluster` and `clusterAllReplicas` table functions can be useful in the following cases:

-   Accessing a specific cluster for data comparison, debugging, and testing.
-   Queries to various ClickHouse clusters and replicas for research purposes.
-   Infrequent distributed requests that are made manually.

Connection settings like `host`, `port`, `user`, `password`, `compression`, `secure` are taken from `<remote_servers>` config section. See details in [Distributed engine](../../engines/table-engines/special/distributed.md).

**See Also**

-   [skip\_unavailable\_shards](../../operations/settings/settings.md#settings-skip_unavailable_shards)
-   [load\_balancing](../../operations/settings/settings.md#settings-load_balancing)
