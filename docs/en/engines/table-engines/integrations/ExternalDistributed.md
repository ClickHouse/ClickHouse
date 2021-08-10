---
toc_priority: 12
toc_title: ExternalDistributed
---

# ExternalDistributed {#externaldistributed}

The `ExternalDistributed` engine allows to perform `SELECT` queries on data that is stored on a remote servers MySQL or PostgreSQL. Accepts [MySQL](../../../engines/table-engines/integrations/mysql.md) or [PostgreSQL](../../../engines/table-engines/integrations/postgresql.md) engines as an argument so sharding is possible.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password');
```

See a detailed description of the [CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query) query.

The table structure can differ from the original table structure:

-   Column names should be the same as in the original table, but you can use just some of these columns and in any order.
-   Column types may differ from those in the original table. ClickHouse tries to [cast](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) values to the ClickHouse data types.

**Engine Parameters**

-   `engine` — The table engine `MySQL` or `PostgreSQL`.
-   `host:port` — MySQL or PostgreSQL server address.
-   `database` — Remote database name.
-   `table` — Remote table name.
-   `user` — User name.
-   `password` — User password.

## Implementation Details {#implementation-details}
	
Supports multiple replicas that must be listed by `|` and shards must be listed by `,`. For example:

```sql
CREATE TABLE test_shards (id UInt32, name String, age UInt32, money UInt32) ENGINE = ExternalDistributed('MySQL', `mysql{1|2}:3306,mysql{3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

When specifying replicas, one of the available replicas is selected for each of the shards when reading. If the connection fails, the next replica is selected, and so on for all the replicas. If the connection attempt fails for all the replicas, the attempt is repeated the same way several times.

You can specify any number of shards and any number of replicas for each shard.

**See Also**

-   [MySQL table engine](../../../engines/table-engines/integrations/mysql.md)
-   [PostgreSQL table engine](../../../engines/table-engines/integrations/postgresql.md)
-   [Distributed table engine](../../../engines/table-engines/special/distributed.md)
