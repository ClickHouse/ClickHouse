---
description: 'Provides real-time access to the tables of a database on a cluster from the server configuration, forwarding `SELECT` and `INSERT` queries to it.'
sidebar_label: 'Cluster'
sidebar_position: 52
slug: /engines/database-engines/cluster
title: 'Cluster'
doc_type: 'reference'
---

# Cluster database engine {#cluster-database-engine}

The `Cluster` database engine provides real-time access to the tables of a database on a cluster from the server configuration. It is the named-cluster counterpart of the [`Remote`](/engines/database-engines/remote) database engine, exactly as the [`cluster`](/sql-reference/table-functions/cluster) table function relates to the [`remote`](/sql-reference/table-functions/remote) table function.

The list of tables and their structure are fetched from the cluster on demand, so the database always reflects its current state. Each table is exposed as a [`Distributed`](/engines/table-engines/special/distributed) storage over the named cluster, which forwards `SELECT` and `INSERT` queries to it.

This is handy for federating several ClickHouse clusters or for plugging a whole cluster into `clickhouse-local` or another cluster without spelling out its addresses: the cluster is defined once in the configuration, complete with per-replica credentials, secure connections and the inter-server secret.

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE cluster_db
ENGINE = Cluster('cluster_name', 'database');
```

**Engine Parameters**

- `cluster_name` — The name of a cluster from the server configuration (see [Clusters](/engines/table-engines/special/distributed#distributed-clusters)), as in the `Distributed` table engine. [Macros](/operations/server-configuration-parameters/settings#macros) such as `{cluster}` are supported and expanded on every access.
- `database` — The name of the database on the cluster.

Unlike the `Remote` database engine, the `Cluster` engine takes no credential arguments and stores no secrets: connections use the per-replica settings of the cluster configuration (user, password, secure connections, compression, the inter-server secret). The cluster is re-resolved from the configuration on every access, so the database follows configuration reloads and [cluster auto-discovery](/operations/cluster-discovery), like a `Distributed` table does. The cluster must exist when the database is created; if it later disappears from the configuration, the server still starts and the database reports the missing cluster until the configuration brings it back.

A replica that points to the current server is treated as a local shard: `SELECT` and `INSERT` are executed directly under the current user — who therefore needs the corresponding privileges on the underlying database and its tables — and the configured cluster credentials are used only for genuinely remote replicas. If the local replica of a shard does not have the database or a table, the lookup falls back to the remote replicas of the shard, like a [`Distributed`](/engines/table-engines/special/distributed) table does.

When the cluster has several shards, each proxy table reads from all of them, but the metadata — the list of the tables and their structure — is taken from an arbitrary shard (a local one is preferred), just like the [`cluster`](/sql-reference/table-functions/cluster) table function does, so that a listing costs a single query instead of one per shard. The shards of a cluster are therefore expected to serve the same set of tables; a table that only some of them have is served by a proxy whose queries then fail on the shards that do not have it. An `INSERT` into a table of a multi-shard database sends each row to a random shard (the proxy `Distributed` tables carry an implicit `rand()` sharding key, respecting the configured shard weights); to pin the shard for a query, set [`insert_shard_id`](/operations/settings/settings#insert_shard_id). The implicit key only distributes the inserted rows: for reading, the table behaves like a `Distributed` table without a sharding key (in particular, [`optimize_skip_unused_shards`](/operations/settings/settings#optimize_skip_unused_shards) and [`force_optimize_skip_unused_shards`](/operations/settings/settings#force_optimize_skip_unused_shards) do not treat it as a shard-pruning key).

`SHOW CREATE TABLE` prints each table as a [`Distributed`](/engines/table-engines/special/distributed) table over the named cluster (including the implicit sharding key of a multi-shard database), so the emitted definition recreates a standalone table with the same behavior:

```sql
CREATE TABLE ... ENGINE = Distributed('cluster_name', 'database', 'table'[, rand()])
```

## Notes {#notes}

- The engine is a read-through view of the cluster: `CREATE TABLE`, `DROP TABLE`, `ALTER` and similar DDL statements against the `Cluster` database are not supported. Manage the schema on the cluster directly, e.g. with [`ON CLUSTER`](/sql-reference/distributed-ddl) DDL queries.
- Access rights are enforced on the remote servers for the users configured in the cluster definition (or for the current user when the cluster uses an inter-server secret), and locally by the usual privileges on the database and its tables.
- The behavioral details of the `Remote` database engine — the visibility rules for the tables of a local shard, the completeness of the listing, error reporting for an unavailable cluster, and chains of proxy databases — apply to the `Cluster` engine as well; see the [notes](/engines/database-engines/remote#notes) there.

## Example {#example}

Create a `Cluster` database that points to the `default` database of the cluster `test_shard_localhost` from the server configuration and use it:

```sql
CREATE DATABASE cluster_db
ENGINE = Cluster('test_shard_localhost', 'default');
```

```sql
SHOW TABLES FROM cluster_db;
```

```text
┌─name─┐
│ t    │
└──────┘
```

```sql
SELECT * FROM cluster_db.t;
```
