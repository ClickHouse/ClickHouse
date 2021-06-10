
# [experimental] Replicated {#replicated}

Supports replication of metadata via DDL log being written to ZooKeeper and executed on all of the replicas for a given database.
One Clickhouse server can have multiple replicated databases running and updating at the same time. But there can't be multiple replicas of the same replicated database.

## Creating a Database {#creating-a-database}
``` sql
    CREATE DATABASE testdb ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**Engine Parameters**

-   `zoo_path` — ZooKeeper path. The same ZooKeeper path corresponds to the same database.
-   `shard_name` — Shard name. Database replicas are grouped into shards by `shard_name`.
-   `replica_name` — Replica name. Replica names must be different for all replicas of the same shard.

Using this engine, creation of Replicated tables requires no ZooKeeper path and replica name parameters. Table's replica name is the same as database replica name. Table's ZooKeeper path is a concatenation of database's ZooKeeper path, `/tables/`, and `UUID` of the table.

## Specifics and recommendations {#specifics-and-recommendations}

Algorithms
Specifics of read and write processes
Examples of tasks
Recommendations for usage
Specifics of data storage

## Usage Example {#usage-example}

The example must show usage and use cases. The following text contains the recommended parts of this section.

Input table:

``` text
```

Query:

``` sql
```

Result:

``` text
```

Follow up with any text to clarify the example.

**See Also** 

-   [link](#)
