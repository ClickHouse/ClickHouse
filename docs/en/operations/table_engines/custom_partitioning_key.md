# Custom Partitioning Key

Partitioning is available for the [MergeTree](mergetree.md) family tables (including [replicated](replication.md) tables). [Materialized views](materializedview.md) based on MergeTree table support partitioning as well.

A partition is a logical combination of records in a table by a specified criterion. You can set a partition by an arbitrary criterion, for example, by month, by day or by event type. Each partition is stored separately in order to simplify manipulations with this data. When accessing the data ClickHouse only uses as small subset of partitions as possible.

The partition is specified in the `PARTITION BY expr` clause when [creating a table](mergetree.md#table_engine-mergetree-creating-a-table). The partition key can be any expression from the table columns. For example, to specify the partitioning by month, use an expression `toYYYYMM(date_column)`:

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

The partition key also can be a tuple of expressions (similar to the [primary key](mergetree.md#primary-keys-and-indexes-in-queries)). For example:

``` sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
```

In this example, we set partitioning by the event types that occurred the current week.

When inserting new data to a table, this data is stored as a separate part (chunk) sorted by the primary key. In 10-15 minutes after inserting, the parts of the same partition are merged into the entire part.

!!! info
    A merge only works for data parts that have the same value for the partitioning expression. It means **you should not make overly granular partitions** (more than about a thousand partitions). Otherwise, the `SELECT` query performs poorly because of an unreasonably large number of files in the file system and open file descriptors.

To view the table parts and partitions, use the [system.parts](../system_tables.md#system_tables-parts) table. For example, let's assume that we have a table `visits` with partitioning by month. Let's perform the `SELECT` query for the `system.parts` table:

``` sql
SELECT 
    partition,
    name, 
    active
FROM system.parts 
WHERE table = 'visits'
```

```
┌─partition─┬─name───────────┬─active─┐
│ 201901    │ 201901_1_3_1   │      0 │
│ 201901    │ 201901_1_9_2   │      1 │
│ 201901    │ 201901_8_8_0   │      0 │
│ 201901    │ 201901_9_9_0   │      0 │
│ 201902    │ 201902_4_6_1   │      1 │
│ 201902    │ 201902_10_10_0 │      1 │
│ 201902    │ 201902_11_11_0 │      1 │
└───────────┴────────────────┴────────┘
```

The `partition` column contains the names of the partitions. There are two partitions in this example: `201901` and `201902`. You can use this column value to specify the partition name in [ALTER ... PARTITION](#alter_manipulations-with-partitions) queries.

The `name` column contains the names of the partition data parts. You can use this column to specify the name of the part in the [ALTER ATTACH PART](#alter_attach-partition) query.
 
The `active` column shows the status of the part. `1` is active; `0` is inactive. The inactive parts are, for example, source parts remaining after merging to a larger part. The corrupted data parts are also indicated as inactive.

Let's break down the name of the first part: `201901_1_3_1`:

- `201901` is the partition name.
- `1` is the minimum number of the data block.
- `3` is the maximum number of the data block.
- `1` is the chunk level (the depth of the merge tree it is formed from).

!!! info
    The parts of old-type tables have the name: `20190117_20190123_2_2_0` (minimum date - maximum date - minimum block number - maximum block number - level).

As you can see in the example, there are several separated parts of the same partition (for example, `201901_1_1_0` and `201901_2_2_0`). It means that these parts are not merged yet. ClickHouse merges the inserted parts of data periodically, approximately in 15 minutes after inserting. Also, you can perform a non-scheduled merge, using the [OPTIMIZE](../../query_language/misc.md#misc_operations-optimize) query. Example:

``` sql
OPTIMIZE TABLE visits PARTITION 201902;
```

```
┌─partition─┬─name───────────┬─active─┐
│ 201901    │ 201901_1_3_1   │      0 │
│ 201901    │ 201901_1_9_2   │      1 │
│ 201901    │ 201901_8_8_0   │      0 │
│ 201901    │ 201901_9_9_0   │      0 │
│ 201902    │ 201902_4_6_1   │      0 │
│ 201902    │ 201902_4_11_2  │      1 │
│ 201902    │ 201902_10_10_0 │      0 │
│ 201902    │ 201902_11_11_0 │      0 │
└───────────┴────────────────┴────────┘
```

Inactive parts will be deleted approximately in 10 minutes after merging. 
 
Another way to view a set of parts and partitions, is to go into the directory of the table: `/var/lib/clickhouse/data/<database>/<table>/`. For example: 

```bash
dev:/var/lib/clickhouse/data/default/visits$ ls -l
total 40
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 201901_1_3_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201901_1_9_2
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_8_8_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_9_9_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_10_10_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_11_11_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:19 201902_4_11_2
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 12:09 201902_4_6_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 detached
```

The folders '201901_1_1_0', '201901_1_7_1' and so on, are the directories of the parts. Each part relates to a corresponding partition and contains data just for a certain month (the table in this example has partitioning by month).

The 'detached' directory contains parts that were detached from the table using the [DETACH](#alter_detach-partition) query. The corrupted parts are also moved to this directory, instead of being deleted. The server does not use the parts from 'detached' directory. You can add, delete, or modify the data in this directory at any time – the server will not know about this until you make the [ATTACH](../../query_language/alter.md#alter_attach-partition) query.

!!! info
    On the operating server, you cannot manually change the set of parts or their data on the file system, since the server will not know about it.
    For non-replicated tables, you can do this when the server is stopped, but we do not recommend it.
    For replicated tables, the set of parts cannot be changed in any case.
    
ClickHouse allows to do operations with the partitions: delete them, copy from one table to another, create a backup. See the list of all operations in a section [Manipulations With Partitions and Parts](../../query_language/alter.md#alter_manipulations-with-partitions). 

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/custom_partitioning_key/) <!--hide-->
