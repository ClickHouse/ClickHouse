<a name="table_engines-custom_partitioning_key"></a>

# Custom Partitioning Key

Starting with version 1.1.54310, you can create tables in the MergeTree family with any partitioning expression (not only partitioning by month).

The partition key can be an expression from the table columns, or a tuple of such expressions (similar to the primary key). The partition key can be omitted. When creating a table, specify the partition key in the ENGINE description with the new syntax:

```
ENGINE [=] Name(...) [PARTITION BY expr] [ORDER BY expr] [SAMPLE BY expr] [SETTINGS name=value, ...]
```

For MergeTree tables, the partition expression is specified after `PARTITION BY`, the primary key after `ORDER BY`, the sampling key after `SAMPLE BY`, and `SETTINGS` can specify `index_granularity` (optional; the default value is 8192), as well as other settings from [MergeTreeSettings.h](https://github.com/yandex/ClickHouse/blob/master/dbms/src/Storages/MergeTree/MergeTreeSettings.h). The other engine parameters are specified in parentheses after the engine name, as previously. Example:

```sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
    PARTITION BY (toMonday(StartDate), EventType)
    ORDER BY (CounterID, StartDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID)
```

The traditional partitioning by month is expressed as `toYYYYMM(date_column)`.

You can't convert an old-style table to a table with custom partitions (only via INSERT SELECT).

After this table is created, merge will only work for data parts that have the same value for the partitioning expression. Note: This means that you shouldn't make overly granular partitions (more than about a thousand partitions), or SELECT will perform poorly.

To specify a partition in ALTER PARTITION commands, specify the value of the partition expression (or a tuple). Constants and constant expressions are supported. Example:

```sql
ALTER TABLE table DROP PARTITION (toMonday(today()), 1)
```

Deletes the partition for the current week with event type 1. The same is true for the OPTIMIZE query. To specify the only partition in a non-partitioned table, specify `PARTITION tuple()`.

Note: For old-style tables, the partition can be specified either as a number `201710` or a string `'201710'`. The syntax for the new style of tables is stricter with types (similar to the parser for the VALUES input format). In addition, ALTER TABLE FREEZE PARTITION uses exact match for new-style tables (not prefix match).

In the `system.parts` table, the `partition` column specifies the value of the partition expression to use in ALTER queries (if quotas are removed). The `name` column should specify the name of the data part that has a new format.

Was: `20140317_20140323_2_2_0` (minimum date - maximum date - minimum block number - maximum block number - level).

Now: `201403_2_2_0`  (partition ID -  minimum block number - maximum block number - level).

The partition ID is its string identifier (human-readable, if possible) that is used for the names of data parts in the file system and in ZooKeeper. You can specify it in ALTER queries in place of the partition key. Example: Partition key `toYYYYMM(EventDate)`; ALTER can specify either `PARTITION 201710` or `PARTITION ID '201710'`.

For more examples, see the tests [`00502_custom_partitioning_local`](https://github.com/yandex/ClickHouse/blob/master/dbms/tests/queries/0_stateless/00502_custom_partitioning_local.sql) and [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/yandex/ClickHouse/blob/master/dbms/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).

