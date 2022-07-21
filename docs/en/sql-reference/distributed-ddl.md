---
sidebar_position: 3
sidebar_label: Distributed DDL
---

# Distributed DDL Queries (ON CLUSTER Clause)

By default the `CREATE`, `DROP`, `ALTER`, and `RENAME` queries affect only the current server where they are executed. In a cluster setup, it is possible to run such queries in a distributed manner with the `ON CLUSTER` clause.

For example, the following query creates the `all_hits` `Distributed` table on each host in `cluster`:

``` sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

In order to run these queries correctly, each host must have the same cluster definition (to simplify syncing configs, you can use substitutions from ZooKeeper). They must also connect to the ZooKeeper servers.

The local version of the query will eventually be executed on each host in the cluster, even if some hosts are currently not available.

:::warning    
The order for executing queries within a single host is guaranteed.
:::

:::warning    
There is a special process inside ClickHouse server -- DDLWorker. It starts when ClickHouse starts. When you execute some SQL with `on cluster` ClickHouse creates tasks in ZooKeeper for each shard in the cluster. DDLWorker on each node takes its own tasks and executes them locally. But because DDLWorker starts with ClickHouse it uses settings from the default profile which were defined when ClickHouse started. There is no way to restart DDLWorker. To apply changes in profile settings you need to restart ClickHouse.
:::
