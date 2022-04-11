---
toc_priority: 50
toc_title: cluster
---

# cluster, clusterAllReplicas {#cluster-clusterallreplicas}

Allows to access all shards in an existing cluster which configured in `remote_servers` section without creating a [Distributed](../../engines/table-engines/special/distributed.md) table. One replica of each shard is queried.

`clusterAllReplicas` function — same as `cluster`, but all replicas are queried. Each replica in a cluster is used as a separate shard/connection.

!!! note "Note"
    All available clusters are listed in the [system.clusters](../../operations/system-tables/clusters.md) table.

**Syntax**

``` sql
cluster('cluster_name', db.table[, sharding_key])
cluster('cluster_name', db, table[, sharding_key])
clusterAllReplicas('cluster_name', db.table[, sharding_key])
clusterAllReplicas('cluster_name', db, table[, sharding_key])
```
**Arguments**

- `cluster_name` – Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers. 
- `db.table` or `db`, `table` - Name of a database and a table.  
- `sharding_key` -  A sharding key. Optional. Needs to be specified if the cluster has more than one shard. 

**Returned value**

The dataset from clusters.

**Using Macros**

`cluster_name` can contain macros — substitution in curly brackets. The substituted value is taken from the [macros](../../operations/server-configuration-parameters/settings.md#macros) section of the server configuration file.

Example:

```sql
SELECT * FROM cluster('{cluster}', default.example_table);
```

**Usage and Recommendations**

Using the `cluster` and `clusterAllReplicas` table functions are less efficient than creating a `Distributed` table because in this case, the server connection is re-established for every request. When processing a large number of queries, please always create the `Distributed` table ahead of time, and do not use the `cluster` and `clusterAllReplicas` table functions.

The `cluster` and `clusterAllReplicas` table functions can be useful in the following cases:

-   Accessing a specific cluster for data comparison, debugging, and testing.
-   Queries to various ClickHouse clusters and replicas for research purposes.
-   Infrequent distributed requests that are made manually.

Connection settings like `host`, `port`, `user`, `password`, `compression`, `secure` are taken from `<remote_servers>` config section. See details in [Distributed engine](../../engines/table-engines/special/distributed.md).

**See Also**

-   [skip_unavailable_shards](../../operations/settings/settings.md#settings-skip_unavailable_shards)
-   [load_balancing](../../operations/settings/settings.md#settings-load_balancing)
