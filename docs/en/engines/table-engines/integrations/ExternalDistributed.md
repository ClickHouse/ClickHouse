---
toc_priority: 12
toc_title: ExternalDistributed
---

# ExternalDistributed {#externaldistributed}

The `ExternalDistributed` engine allows to perform `SELECT` and `INSERT` queries on data that is stored on a remote servers MySQL or PostgreSQL. Accepts [MySQL](../../../engines/table-engines/integrations/mysql.md) or [PostgreSQL](../../../engines/table-engines/integrations/postgresql.md) engines as an argument so sharding is possible.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password'[, `schema`]);
```

See a detailed description of the [CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query) query.

The table structure can differ from the original table structure:

-   Column names should be the same as in the original table, but you can use just some of these columns and in any order.
-   Column types may differ from those in the original table. ClickHouse tries to [cast](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) values to the ClickHouse data types.
-   Setting `external_table_functions_use_nulls` defines how to handle Nullable columns. Default is 1, if 0 - table function will not make nullable columns and will insert default values instead of nulls. This is also applicable for null values inside array data types.

**Engine Parameters**

-   `engine` — The table engine: `MySQL` or `PostgreSQL`.
-   `host:port` — MySQL or PostgreSQL server address.
-   `database` — Remote database name.
-   `table` — Remote table name.
-   `user` — User name.
-   `password` — User password.
-   `schema` — Non-default table schema. Optional.

## Implementation Details {#implementation-details}
	
Supports multiple replicas that must be listed by a character `|`. For example:

```sql
CREATE TABLE test_shards (id UInt32, name String, age UInt32, money UInt32) ENGINE = ExternalDistributed('MySQL', `mysql{1|2}:3306,mysql{3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

When specifying replicas, one of the available replicas will be selected for each of the shards when reading. You can configure the algorithm for load balancing (the preference for which replica to access) – see the [load_balancing](../../../operations/settings/settings.md#settings-load_balancing) setting. If the connection with the server is not established, there will be an attempt to connect with a short timeout. If the connection failed, the next replica will be selected, and so on for all the replicas. If the connection attempt failed for all the replicas, the attempt will be repeated the same way several times. This works in favor of resiliency, but does not provide complete fault tolerance: a remote server might accept the connection, but might not work, or work poorly.

You can specify just one of the shards (in this case, query processing should be called remote, rather than distributed) or up to any number of shards. In each shard, you can specify from one to any number of replicas. You can specify a different number of replicas for each shard.

Each shard can have a weight defined in the config file. By default, the weight is equal to one. Data is distributed across shards in the amount proportional to the shard weight. For example, if there are two shards and the first has a weight of 9 while the second has a weight of 10, the first will be sent 9 / 19 parts of the rows, and the second will be sent 10 / 19.

To select the shard that a row of data is sent to, the sharding expression is analyzed, and its remainder is taken from dividing it by the total weight of the shards. The row is sent to the shard that corresponds to the half-interval of the remainders from `prev_weight` to `prev_weights + weight`, where `prev_weights` is the total weight of the shards with the smallest number, and `weight` is the weight of this shard. For example, if there are two shards, and the first has a weight of 9 while the second has a weight of 10, the row will be sent to the first shard for the remainders from the range \[0, 9), and to the second for the remainders from the range \[9, 19).

The sharding expression can be any expression from constants and table columns that returns an integer. For example, you can use the expression `rand()` for random distribution of data, or `UserID` for distribution by the remainder from dividing the user’s ID (then the data of a single user will reside on a single shard, which simplifies running IN and JOIN by users). If one of the columns is not distributed evenly enough, you can wrap it in a hash function: [intHash64](../../../sql-reference/functions/hash-functions.md#inthash64)(UserID).

A simple reminder from the division is a limited solution for sharding and is not always appropriate. It works for medium and large volumes of data (dozens of servers), but not for very large volumes of data (hundreds of servers or more). In the latter case, use the sharding scheme required by the subject area, rather than using entries in Distributed tables.

`SELECT` queries are sent to all the shards and work regardless of how data is distributed across the shards (they can be distributed completely randomly). When you add a new shard, you do not have to transfer the old data to it. You can write new data with a heavier weight – the data will be distributed slightly unevenly, but queries will work correctly and efficiently.

You should be concerned about the sharding scheme in the following cases:

-   Queries are used that require joining data (`IN` or `JOIN`) by a specific key. If data is sharded by this key, you can use local `IN` or `JOIN` instead of `GLOBAL IN` or `GLOBAL JOIN`, which is much more efficient.
-   A large number of servers is used (hundreds or more) with a large number of small queries (queries of individual clients - websites, advertisers, or partners). In order for the small queries to not affect the entire cluster, it makes sense to locate data for a single client on a single shard. Alternatively, as we have done in Yandex.Metrica, you can set up bi-level sharding: divide the entire cluster into "layers", where a layer may consist of multiple shards. Data for a single client is located on a single layer, but shards can be added to a layer as necessary, and data is randomly distributed within them. Distributed tables are created for each layer, and a single shared distributed table is created for global queries.

**See Also**

-   [MySQL table engine](../../../engines/table-engines/integrations/mysql.md)
-   [PostgreSQL table engine](../../../engines/table-engines/integrations/postgresql.md)
-   [URL Table Engine](../../../engines/table-engines/integrations/url.md)
-   [Distributed Table Engine](../../../engines/table-engines/special/distributed.md)
