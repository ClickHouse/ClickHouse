---
toc_priority: 50
toc_title: cluster
---

# cluster, clusterAllReplicas {#cluster-clusterallreplicas}

Allows you to access all shards in existing cluster which present in `system.clusters` and configured in `remote_servers` section without creating a `Distributed` table.
`clusterAllReplicas` - same as `cluster` but each replica in cluster used as separated shard/connection. 


Signatures:

``` sql
cluster('cluster_name', db.table)
cluster('cluster_name', db, table)
clusterAllReplicas('cluster_name', db.table)
clusterAllReplicas('cluster_name', db, table)
```

`cluster_name` – A name of cluster which shall exists in `system.clusters` table and assume set of addresses and connection parameters to remote and local servers. 

Using the `cluster` and `clusterAllReplicas` table functions is less optimal than creating a `Distributed` table, because in this case, the server connection is re-established for every request. When processing a large number of queries, please always create the `Distributed` table ahead of time, and don’t use the `cluster` and `clusterAllReplicas` table functions.

The `cluster` and `clusterAllReplicas` table function can be useful in the following cases:

-   Accessing a specific cluster for data comparison, debugging, and testing.
-   Queries to various ClickHouse clusters and replicas for research purposes.
-   Infrequent distributed requests that are made manually.

Connection settings like `user`, `password`, `host`, `post`, `compression`, `secure` gets from `<remote_servers>` config section. See details in [Distributed engine](../../engines/table-engines/special/distributed.md)

**See Also**

-   [skip\_unavailable\_shards](../../operations/settings/settings.md#settings-skip_unavailable_shards)
-   [load\_balancing](../../operations/settings/settings.md#settings-load_balancing)