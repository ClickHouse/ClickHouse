---
description: 'This engine allows integrating ClickHouse with RocksDB'
sidebar_label: 'EmbeddedRocksDB'
sidebar_position: 50
slug: /engines/table-engines/integrations/embedded-rocksdb
title: 'EmbeddedRocksDB Engine'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# EmbeddedRocksDB Engine

<CloudNotSupportedBadge />

This engine allows integrating ClickHouse with [RocksDB](http://rocksdb.org/).

## Creating a Table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = EmbeddedRocksDB([ttl, rocksdb_dir, read_only]) PRIMARY KEY(primary_key_name)
[ SETTINGS name=value, ... ]
```

Engine parameters:

- `ttl` - time to live for values. TTL is accepted in seconds. If TTL is 0, regular RocksDB instance is used (without TTL).
- `rocksdb_dir` - path to the directory of an existed RocksDB or the destination path of the created RocksDB. Open the table with the specified `rocksdb_dir`.
- `read_only` - when `read_only` is set to true, read-only mode is used. For storage with TTL, compaction will not be triggered (neither manual nor automatic), so no expired entries are removed.
- `primary_key_name` – any column name in the column list.
- `primary key` must be specified, it supports only one column in the primary key. The primary key will be serialized in binary as a `rocksdb key`.
- columns other than the primary key will be serialized in binary as `rocksdb` value in corresponding order.
- queries with key `equals` or `in` filtering will be optimized to multi keys lookup from `rocksdb`.

Engine settings:

- `optimize_for_bulk_insert` – Table is optimized for bulk insertions (insert pipeline will create SST files and import to rocksdb database instead of writing to memtables); default value: `1`.
- `bulk_insert_block_size` - Minimum size of SST files (in term of rows) created by bulk insertion; default value: `1048449`.

Example:

```sql
CREATE TABLE test
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

## Metrics {#metrics}

There is also `system.rocksdb` table, that expose rocksdb statistics:

```sql
SELECT
    name,
    value
FROM system.rocksdb

┌─name──────────────────────┬─value─┐
│ no.file.opens             │     1 │
│ number.block.decompressed │     1 │
└───────────────────────────┴───────┘
```

## Configuration {#configuration}

You can also change any [rocksdb options](https://github.com/facebook/rocksdb/wiki/Option-String-and-Option-Map) using config:

```xml
<rocksdb>
    <options>
        <max_background_jobs>8</max_background_jobs>
    </options>
    <column_family_options>
        <num_levels>2</num_levels>
    </column_family_options>
    <tables>
        <table>
            <name>TABLE</name>
            <options>
                <max_background_jobs>8</max_background_jobs>
            </options>
            <column_family_options>
                <num_levels>2</num_levels>
            </column_family_options>
        </table>
    </tables>
</rocksdb>
```

By default trivial approximate count optimization is turned off, which might affect the performance `count()` queries. To enable this
optimization set up `optimize_trivial_approximate_count_query = 1`. Also, this setting affects `system.tables` for EmbeddedRocksDB engine,
turn on the settings to see approximate values for `total_rows` and `total_bytes`.

## Supported operations {#supported-operations}

### Inserts {#inserts}

When new rows are inserted into `EmbeddedRocksDB`, if the key already exists, the value will be updated, otherwise a new key is created.

Example:

```sql
INSERT INTO test VALUES ('some key', 1, 'value', 3.2);
```

### Deletes {#deletes}

Rows can be deleted using `DELETE` query or `TRUNCATE`.

```sql
DELETE FROM test WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE test DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE test;
```

### Updates {#updates}

Values can be updated using the `ALTER TABLE` query. The primary key cannot be updated.

```sql
ALTER TABLE test UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

### Joins {#joins}

A special `direct` join with EmbeddedRocksDB tables is supported.
This direct join avoids forming a hash table in memory and accesses
the data directly from the EmbeddedRocksDB.

With large joins you may see much lower memory usage with direct joins
because the hash table is not created.

To enable direct joins:
```sql
SET join_algorithm = 'direct, hash'
```

:::tip
When the `join_algorithm` is set to `direct, hash`, direct joins will be used
when possible, and hash otherwise.
:::

#### Example {#example}

##### Create and populate an EmbeddedRocksDB table {#create-and-populate-an-embeddedrocksdb-table}
```sql
CREATE TABLE rdb
(
    `key` UInt32,
    `value` Array(UInt32),
    `value2` String
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

```sql
INSERT INTO rdb
    SELECT
        toUInt32(sipHash64(number) % 10) AS key,
        [key, key+1] AS value,
        ('val2' || toString(key)) AS value2
    FROM numbers_mt(10);
```

##### Create and populate a table to join with table `rdb` {#create-and-populate-a-table-to-join-with-table-rdb}

```sql
CREATE TABLE t2
(
    `k` UInt16
)
ENGINE = TinyLog
```

```sql
INSERT INTO t2 SELECT number AS k
FROM numbers_mt(10)
```

##### Set the join algorithm to `direct`{#set-the-join-algorithm-to-direct}

```sql
SET join_algorithm = 'direct'
```

##### An INNER JOIN {#an-inner-join}
```sql
SELECT *
FROM
(
    SELECT k AS key
    FROM t2
) AS t2
INNER JOIN rdb ON rdb.key = t2.key
ORDER BY key ASC
```
```response
┌─key─┬─rdb.key─┬─value──┬─value2─┐
│   0 │       0 │ [0,1]  │ val20  │
│   2 │       2 │ [2,3]  │ val22  │
│   3 │       3 │ [3,4]  │ val23  │
│   6 │       6 │ [6,7]  │ val26  │
│   7 │       7 │ [7,8]  │ val27  │
│   8 │       8 │ [8,9]  │ val28  │
│   9 │       9 │ [9,10] │ val29  │
└─────┴─────────┴────────┴────────┘
```

### More information on Joins {#more-information-on-joins}
- [`join_algorithm` setting](/operations/settings/settings.md#join_algorithm)
- [JOIN clause](/sql-reference/statements/select/join.md)
