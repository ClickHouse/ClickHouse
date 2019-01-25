# Custom Partitioning Key

Partitioning is available for the following table engines:

- [MergeTree](../operations/table_engines/mergetree.md) family tables (including
[replicated](../operations/table_engines/replication.md) tables).
- [MaterializedView](../operations/table_engines/materializedview.md). 

The partition key can be an expression from the table columns, or a tuple of such expressions (similar to the primary key). The partition key can be omitted. When creating a table, specify the partition key in the ENGINE description with the new syntax:

```
ENGINE [=] Name(...) [PARTITION BY expr] [ORDER BY expr] [SAMPLE BY expr] [SETTINGS name=value, ...]
```

For MergeTree tables, the partition expression is specified after `PARTITION BY`, the primary key after `ORDER BY`, the sampling key after `SAMPLE BY`, and `SETTINGS` can specify `index_granularity` (optional; the default value is 8192), as well as other settings from [MergeTreeSettings.h](https://github.com/yandex/ClickHouse/blob/master/dbms/src/Storages/MergeTree/MergeTreeSettings.h). The other engine parameters are specified in parentheses after the engine name, as previously. Example:

``` sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
    PARTITION BY (toMonday(StartDate), EventType)
    ORDER BY (CounterID, StartDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID)
```

The traditional partitioning by month is expressed as `toYYYYMM(date_column)`.

You can't convert an old-style table to a table with custom partitions (only via INSERT SELECT).

After this table is created, merge will only work for data parts that have the same value for the partitioning expression. Note: This means that you shouldn't make overly granular partitions (more than about a thousand partitions), or SELECT will perform poorly.

ClickHouse allows to do manipulations with partitions: delete, attach or create a backup. You can see the list of all queries in a section [Manipulations With Partitions and Parts](../../query_language/alter.md#alter_manipulations-with-partitions). 
[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/custom_partitioning_key/) <!--hide-->
