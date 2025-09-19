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

[Apache Avro](https://avro.apache.org/) is a row-oriented serialization format that uses binary encoding for efficient data processing. The `Avro` format supports reading and writing [Avro data files](https://avro.apache.org/docs/++version++/specification/#object-container-files). This format expects self-describing messages with an embedded schema. If you're using Avro with a schema registry, refer to the [`AvroConfluent`](./AvroConfluent.md) format.

## Data type mapping {#data-type-mapping}

<DataTypeMapping/>

## Format settings {#format-settings}

| Setting                                     | Description                                                                                         | Default |
|---------------------------------------------|-----------------------------------------------------------------------------------------------------|---------|
| `input_format_avro_allow_missing_fields`    | Whether to use a default value instead of throwing an error when a field is not found in the schema. | `0`     |
| `input_format_avro_null_as_default`         | Whether to use a default value instead of throwing an error when inserting a `null` value into a non-nullable column. |   `0`   |
| `output_format_avro_codec`                  | Compression algorithm for Avro output files. Possible values: `null`, `deflate`, `snappy`, `zstd`.            |         |
| `output_format_avro_sync_interval`          | Sync marker frequency in Avro files (in bytes). | `16384` |
| `output_format_avro_string_column_pattern`  | Regular expression to identify `String` columns for Avro string type mapping. By default, ClickHouse `String` columns are written as Avro `bytes` type.                                 |         |
| `output_format_avro_rows_in_file`           | Maximum number of rows per Avro output file. When this limit is reached, a new file is created (if the storage system supports file splitting).                                                         | `1`     |

## Examples {#examples}

### Reading Avro data {#reading-avro-data}

To read data from an Avro file into a ClickHouse table:

```bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

The root schema of the ingested Avro file must be of type `record`.

To find the correspondence between table columns and fields of Avro schema, ClickHouse compares their names. 
This comparison is case-sensitive and unused fields are skipped.

Data types of ClickHouse table columns can differ from the corresponding fields of the Avro data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#cast) the data to the corresponding column type.

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
DESCRIBE url('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits.avro','Avro);

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
