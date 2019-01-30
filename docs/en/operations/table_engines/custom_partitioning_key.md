# Custom Partitioning Key

Partitioning is available for the following table engines:

- [MergeTree](mergetree.md) family tables (including
[replicated](replication.md) tables).
- [MaterializedView](materializedview.md). 

A partition is a logic combination of records in a table by a specified criterion. You can set a partition by an arbitrary criterion. For example: by month, by day, by event type and so on. Each partition is stored separately in order to simplify manipulations with this data. When accessing the data ClickHouse only uses the corresponding partition.

The partition is specified in the clause `PARTITION BY expr` when [creating a table](mergetree.md#table_engine-mergetree-creating-a-table). The partition key can be any expression from the table columns. To specify the traditional partitioning by month, use an expression `toYYYYMM(date_column)`. For example:

``` sql
CREATE TABLE visits
(
    VisitDate Date, 
    Hour UInt8, 
    ClientID UUID
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(Date)
ORDER BY Hour
```

The partition key also can be a tuple of expressions (similar to the primary key). For example:

``` sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
```

In this example, we set partitioning by the event types that occurred the current week.

After a table is created, a merge only works for data parts that have the same value for the partitioning expression. It means **you should not make overly granular partitions** (more than about a thousand partitions), or the `SELECT` query performs poorly.

ClickHouse allows to do operations with the partitions: delete them, copy from one table to another, create a backup. See the list of all operations in a section [Manipulations With Partitions and Parts](../../query_language/alter.md#alter_manipulations-with-partitions). 

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/custom_partitioning_key/) <!--hide-->
