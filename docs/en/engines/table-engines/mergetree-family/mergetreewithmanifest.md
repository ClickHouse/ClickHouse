---
description: 'MergeTreeWithManifest inherits from MergeTree and uses manifest storage to manage part metadata, avoiding filesystem scans during table startup.'
sidebar_label: 'MergeTreeWithManifest'
sidebar_position: 45
slug: /engines/table-engines/mergetree-family/mergetreewithmanifest
title: 'MergeTreeWithManifest table engine'
doc_type: 'reference'
---

# MergeTreeWithManifest table engine

The `MergeTreeWithManifest` engine inherits from [MergeTree](/engines/table-engines/mergetree-family/mergetree.md) and adds manifest storage functionality to manage part metadata. Instead of scanning the filesystem to discover data parts during table startup, `MergeTreeWithManifest` uses a key-value store (manifest storage) to track part metadata, which significantly improves startup performance for tables with large numbers of parts.

## Key features

- **Fast startup**: Avoids filesystem scans by using manifest storage to track part metadata
- **Crash recovery**: Supports redo logic for incomplete operations (PreCommit, PreRemove, PreDetach states)
- **Full MergeTree compatibility**: Inherits all features and capabilities from the base `MergeTree` engine

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = MergeTreeWithManifest('manifest_storage_type')
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

For a description of request parameters, see [request description](/sql-reference/statements/create/table.md).

### Parameters of MergeTreeWithManifest {#parameters-of-mergetreewithmanifest}

#### manifest_storage_type {#manifest-storage-type}

`manifest_storage_type` — The type of manifest storage backend to use. Required parameter.

Currently supported values:
- `'rocksdb'` — Uses RocksDB as the manifest storage backend

The manifest storage is created in a dedicated directory under the server's main data path: `<server_path>/manifest/<database>/<table>/`

### Query clauses {#query-clauses}

When creating a `MergeTreeWithManifest` table, the same [clauses](/engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) are required as when creating a `MergeTree` table.

:::note
The `assign_part_uuids` setting is automatically enabled when using `MergeTreeWithManifest`. This is required because the manifest storage uses part UUIDs as keys to track part metadata. The `assign_part_uuids` setting is a standard MergeTree setting that can also be enabled manually for regular MergeTree tables.
:::

## Usage example {#usage-example}

Create a table with `MergeTreeWithManifest`:

```sql
CREATE TABLE simple_manifest_table
(
    `id` UInt32,
    `name` String,
    `score` Int32
)
ENGINE = MergeTreeWithManifest('rocksdb')
ORDER BY id
SETTINGS index_granularity = 8192;
```

Insert data:

```sql
INSERT INTO simple_manifest_table VALUES
(1, 'Alice', 95),
(2, 'Bob', 87),
(3, 'Charlie', 92);
```

Query data:

```sql
SELECT * FROM simple_manifest_table ORDER BY id;
```

```text
┌─id─┬─name─────┬─score─┐
│  1 │ Alice    │    95 │
│  2 │ Bob      │    87 │
│  3 │ Charlie  │    92 │
└────┴──────────┴───────┘
```

## How it works {#how-it-works}

### Manifest storage

The manifest storage maintains a mapping between part UUIDs and their metadata (name, state, disk name, disk path). When a part is created, committed, removed, or detached, the manifest is updated accordingly.

### Part loading

During table startup, instead of scanning the filesystem for data parts, `MergeTreeWithManifest`:
1. Reads all entries from the manifest storage
2. Filters entries to active parts only
3. Handles incomplete operations (redo logic) for crash recovery
4. Loads parts using the metadata from the manifest

### Crash recovery

`MergeTreeWithManifest` implements redo logic for three intermediate states:
- **PreCommit**: Temporary parts that were being committed
- **PreRemove**: Parts that were being removed
- **PreDetach**: Parts that were being moved to the detached directory

On restart, these states are automatically resolved by either completing the operation or cleaning up incomplete data.

## Limitations {#limitations}

- Requires RocksDB to be available in the build (manifest storage type `'rocksdb'`)
- The `assign_part_uuids` setting is automatically enabled (required for manifest tracking)
- Manifest storage must be accessible and properly initialized

## See also {#see-also}

- [MergeTree table engine](/engines/table-engines/mergetree-family/mergetree.md)
- [Data replication](/engines/table-engines/mergetree-family/replication.md)
