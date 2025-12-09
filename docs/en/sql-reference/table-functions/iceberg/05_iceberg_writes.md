---
description: 'Writes into iceberg table'
sidebar_label: 'Iceberg writes (experimental)'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg-writes-into-iceberg
title: 'Writes into iceberg tables'
doc_type: 'reference'
---

# Writes into iceberg tables {#writes-into-iceberg-table}

Starting from version 25.7, ClickHouse supports modifications of user’s Iceberg tables.

Currently, this is an experimental feature, so you first need to enable it:

```sql
SET allow_experimental_insert_into_iceberg = 1;
```

## Creating table {#create-iceberg-table}

To create your own empty Iceberg table, use the same commands as for reading, but specify the schema explicitly.
Writes supports all data formats from iceberg specification, such as Parquet, Avro, ORC.

### Example {#example-iceberg-writes-create}

```sql
CREATE TABLE iceberg_writes_example
(
    x Nullable(String),
    y Nullable(Int32)
)
ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/')
```

Note: To create a version hint file, enable the `iceberg_use_version_hint` setting.
If you want to compress the metadata.json file, specify the codec name in the `iceberg_metadata_compression_method` setting.

## INSERT {#writes-inserts}

After creating a new table, you can insert data using the usual ClickHouse syntax.

### Example {#example-iceberg-writes-insert}

```sql
INSERT INTO iceberg_writes_example VALUES ('Pavel', 777), ('Ivanov', 993);

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Pavel
y: 777

Row 2:
──────
x: Ivanov
y: 993
```

## DELETE {#iceberg-writes-delete}

Deleting extra rows in the merge-on-read format is also supported in ClickHouse.
This query will create a new snapshot with position delete files.

NOTE: If you want to read your tables in the future with other Iceberg engines (such as Spark), you need to disable the settings `output_format_parquet_use_custom_encoder` and `output_format_parquet_parallel_encoding`.
This is because Spark reads these files by parquet field-ids, while ClickHouse does not currently support writing field-ids when these flags are enabled.
We plan to fix this behavior in the future.

### Example {#example-iceberg-writes-delete}

```sql
ALTER TABLE iceberg_writes_example DELETE WHERE x != 'Ivanov';

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

## Schema evolution {#iceberg-writes-schema-evolution}

ClickHouse allows you to add, drop, or modify columns with simple types (non-tuple, non-array, non-map).

### Example {#example-iceberg-writes-evolution}

```sql
ALTER TABLE iceberg_writes_example MODIFY COLUMN y Nullable(Int64);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

ALTER TABLE iceberg_writes_example ADD COLUMN z Nullable(Int32);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64),                                 ↴│
   │↳    `z` Nullable(Int32)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
z: ᴺᵁᴸᴸ

ALTER TABLE iceberg_writes_example DROP COLUMN z;
SHOW CREATE TABLE iceberg_writes_example;
   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

## Compaction {#iceberg-writes-compaction}

ClickHouse supports compaction of iceberg tables.
Currently, it can merge position delete files into data files while updating metadata.
Previous snapshot IDs and timestamps remain unchanged, so the [time-travel feature](/sql-reference/table-functions/iceberg-time-travel) can still be used with the same values.

How to use it:

```sql
SET allow_experimental_iceberg_compaction = 1

OPTIMIZE TABLE iceberg_writes_example;

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```