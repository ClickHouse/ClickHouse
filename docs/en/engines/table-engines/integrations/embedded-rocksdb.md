---
slug: /en/engines/table-engines/integrations/embedded-rocksdb
sidebar_position: 9
sidebar_label: EmbeddedRocksDB
---

# EmbeddedRocksDB Engine

This engine allows integrating ClickHouse with [rocksdb](http://rocksdb.org/).

## Creating a Table {#table_engine-EmbeddedRocksDB-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = EmbeddedRocksDB([ttl, rocksdb_dir, read_only]) PRIMARY KEY(primary_key_name)
```

Engine parameters:

- `ttl` - time to live for values. TTL is accepted in seconds. If TTL is 0, regular RocksDB instance is used (without TTL).
- `rocksdb_dir` - path to the directory of an existed RocksDB or the destination path of the created RocksDB. Open the table with the specified `rocksdb_dir`.
- `read_only` - when `read_only` is set to true, read-only mode is used. For storage with TTL, compaction will not be triggered (neither manual nor automatic), so no expired entries are removed.
- `primary_key_name` – any column name in the column list.
- `primary key` must be specified, it supports only one column in the primary key. The primary key will be serialized in binary as a `rocksdb key`.
- columns other than the primary key will be serialized in binary as `rocksdb` value in corresponding order.
- queries with key `equals` or `in` filtering will be optimized to multi keys lookup from `rocksdb`.

Example:

``` sql
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

## Metrics

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

## Configuration

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

## Supported operations {#table_engine-EmbeddedRocksDB-supported-operations}

### Inserts

When new rows are inserted into `EmbeddedRocksDB`, if the key already exists, the value will be updated, otherwise a new key is created.

Example:

```sql
INSERT INTO test VALUES ('some key', 1, 'value', 3.2);
```

### Deletes

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

### Updates

Values can be updated using the `ALTER TABLE` query. The primary key cannot be updated.

```sql
ALTER TABLE test UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```
