---
alias: []
description: 'Documentation for the Avro format'
input_format: true
keywords: ['Avro']
output_format: true
slug: /interfaces/formats/Avro
title: 'Avro'
doc_type: 'reference'
---

import DataTypeMapping from './_snippets/data-types-matching.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache Avro](https://avro.apache.org/) is a row-oriented serialization format that uses binary encoding for efficient data processing. The `Avro` format supports reading and writing [Avro data files](https://avro.apache.org/docs/current/specification/#object-container-files). This format expects self-describing messages with an embedded schema. If you're using Avro with a schema registry, refer to the [`AvroConfluent`](./AvroConfluent.md) format.

## Data type mapping {#data-type-mapping}

<DataTypeMapping/>

## Format settings {#format-settings}

| Setting                                     | Description                                                                                         | Default |
|---------------------------------------------|-----------------------------------------------------------------------------------------------------|---------|
| `input_format_avro_allow_missing_fields`    | Whether to use a default value instead of throwing an error when a field is not found in the schema. | `0`     |
| `input_format_avro_null_as_default`         | Whether to use a default value instead of throwing an error when inserting a `null` value into a non-nullable column. |   `0`   |
| `input_format_avro_union_type_name`         | Expose the active union branch name as a `$name` sub-column, and each branch of a multi-branch union as a named sub-column. See [Union sub-columns](#union-sub-columns). |   `0`   |
| `output_format_avro_codec`                  | Compression algorithm for Avro output files. Possible values: `null`, `deflate`, `snappy`, `zstd`.            |         |
| `output_format_avro_sync_interval`          | Sync marker frequency in Avro files (in bytes). | `16384` |
| `output_format_avro_string_column_pattern`  | Regular expression to identify `String` columns for Avro string type mapping. By default, ClickHouse `String` columns are written as Avro `bytes` type.                                 |         |
| `output_format_avro_rows_in_file`           | Maximum number of rows per Avro output file. When this limit is reached, a new file is created (if the storage system supports file splitting).                                                         | `1`     |

## Union sub-columns {#union-sub-columns}

Avro unions carry no indication in the data of which branch a given record used —
that information is in the encoded branch index, not in the value. With
[`input_format_avro_union_type_name`](/operations/settings/settings-formats.md/#input_format_avro_union_type_name)
enabled, each union-typed field gets extra sub-columns that make the branch
addressable by name.

Given this Avro schema:

```json
{
  "type": "record", "name": "Event",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "payload", "type": [
      "null",
      {"type": "record", "name": "TypeB", "fields": [{"name": "y", "type": "string"}]},
      {"type": "record", "name": "TypeC", "fields": [{"name": "z", "type": "double"}]}
    ]}
  ]
}
```

the inferred structure is:

```sql
DESCRIBE file('events.avro') SETTINGS input_format_avro_union_type_name = 1;
```

```text
id              Int32
payload         Variant(Tuple(y String), Tuple(z Float64))
payload.$name   Nullable(String)
payload.TypeB   Nullable(Tuple(y String))
payload.TypeC   Nullable(Tuple(z Float64))
```

- `payload.$name` holds the active branch name for each row, or `NULL` for the
  null branch.
- `payload.TypeB` and `payload.TypeC` hold that branch's value on the rows where
  it is active, and `NULL` on all other rows.

This makes it possible to filter and project by branch:

```sql
SELECT id, `payload.TypeB`
FROM file('events.avro')
WHERE `payload.$name` = 'TypeB'
SETTINGS input_format_avro_union_type_name = 1;
```

### Which unions get branch sub-columns {#which-unions-get-branch-sub-columns}

Only unions with more than one non-null branch, which map to `Variant`, get
branch sub-columns. A union such as `["null", "TypeA"]` maps to `Nullable(TypeA)`,
whose value is already directly accessible, so only its `$name` sub-column is
exposed.

### Nested unions {#nested-unions}

If a branch is a record containing a union field, that inner union's `$name` is
exposed one level deeper:

```text
payload.TypeA.inner.$name   Nullable(String)
```

This currently covers the first qualifying nested union field per branch, and
only the `$name` sub-column — inner branch values are not exposed as separate
sub-columns, though they remain reachable inside `payload.TypeA`.

The nested `$name` can be selected on its own, or together with the union value
column (`payload` above). Selecting it together with the outer union's own
`$name` sub-column but without the value column is not supported, and fails with
`THERE_IS_NO_COLUMN`.

### Declaring the sub-columns explicitly {#declaring-union-sub-columns}

When the structure is given explicitly instead of inferred, the sub-columns have
to be declared too. A branch sub-column must be `Nullable`, because it is `NULL`
on every row where the union holds a different branch; declaring it non-nullable
is rejected. Note that [`Nullable(Tuple(...))`](/sql-reference/data-types/tuple#nullable-tuple)
is supported when `enable_nullable_tuple_type = 1` is enabled.

```sql
SELECT id, `payload.$name`
FROM file('events.avro', 'Avro', 'id Int32, `payload.$name` Nullable(String)')
SETTINGS input_format_avro_union_type_name = 1;
```

## Examples {#examples}

### Reading Avro data {#reading-avro-data}

To read data from an Avro file into a ClickHouse table:

```bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

The root schema of the ingested Avro file must be of type `record`.

To find the correspondence between table columns and fields of Avro schema, ClickHouse compares their names. 
This comparison is case-sensitive and unused fields are skipped.

Data types of ClickHouse table columns can differ from the corresponding fields of the Avro data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#CAST) the data to the corresponding column type.

While importing data, when a field is not found in the schema and setting [`input_format_avro_allow_missing_fields`](/operations/settings/settings-formats.md/#input_format_avro_allow_missing_fields) is enabled, the default value will be used instead of throwing an error.

### Writing Avro data {#writing-avro-data}

To write data from a ClickHouse table into an Avro file:

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

Column names must:

- Start with `[A-Za-z_]`
- Be followed by only `[A-Za-z0-9_]`

The output compression and sync interval for Avro files can be configured using the [`output_format_avro_codec`](/operations/settings/settings-formats.md/#output_format_avro_codec) and [`output_format_avro_sync_interval`](/operations/settings/settings-formats.md/#output_format_avro_sync_interval) settings, respectively.

### Inferring the Avro schema {#inferring-the-avro-schema}

Using the ClickHouse [`DESCRIBE`](/sql-reference/statements/describe-table) function, you can quickly view the inferred format of an Avro file like the following example. 
This example includes the URL of a publicly accessible Avro file in the ClickHouse S3 public bucket:

```sql
DESCRIBE url('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits.avro', 'Avro');

┌─name───────────────────────┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ WatchID                    │ Int64           │              │                    │         │                  │                │
│ JavaEnable                 │ Int32           │              │                    │         │                  │                │
│ Title                      │ String          │              │                    │         │                  │                │
│ GoodEvent                  │ Int32           │              │                    │         │                  │                │
│ EventTime                  │ Int32           │              │                    │         │                  │                │
│ EventDate                  │ Date32          │              │                    │         │                  │                │
│ CounterID                  │ Int32           │              │                    │         │                  │                │
│ ClientIP                   │ Int32           │              │                    │         │                  │                │
│ ClientIP6                  │ FixedString(16) │              │                    │         │                  │                │
│ RegionID                   │ Int32           │              │                    │         │                  │                │
...
│ IslandID                   │ FixedString(16) │              │                    │         │                  │                │
│ RequestNum                 │ Int32           │              │                    │         │                  │                │
│ RequestTry                 │ Int32           │              │                    │         │                  │                │
└────────────────────────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
