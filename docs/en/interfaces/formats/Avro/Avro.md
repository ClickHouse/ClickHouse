---
alias: []
description: 'Documentation for the Avro format'
input_format: true
keywords: ['Avro']
output_format: true
slug: /interfaces/formats/Avro
title: 'Avro'
---

import DataTypesMatching from './_snippets/data-types-matching.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache Avro](https://avro.apache.org/) is a row-oriented data serialization framework developed within Apache's Hadoop project.
ClickHouse's `Avro` format supports reading and writing [Avro data files](https://avro.apache.org/docs/current/spec.html#Object+Container+Files).

## Data Types Matching {#data-types-matching}

<DataTypesMatching/>

## Example Usage {#example-usage}

### Inserting Data {#inserting-data}

To insert data from an Avro file into a ClickHouse table:

```bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

The root schema of the ingested Avro file must be of type `record`.

To find the correspondence between table columns and fields of Avro schema, ClickHouse compares their names. 
This comparison is case-sensitive and unused fields are skipped.

Data types of ClickHouse table columns can differ from the corresponding fields of the Avro data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#cast) the data to the corresponding column type.

While importing data, when a field is not found in the schema and setting [`input_format_avro_allow_missing_fields`](/operations/settings/settings-formats.md/#input_format_avro_allow_missing_fields) is enabled, the default value will be used instead of throwing an error.

### Selecting Data {#selecting-data}

To select data from a ClickHouse table into an Avro file:

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

Column names must:

- Start with `[A-Za-z_]`
- Be followed by only `[A-Za-z0-9_]`

Output Avro file compression and sync interval can be configured with settings [`output_format_avro_codec`](/operations/settings/settings-formats.md/#output_format_avro_codec) and [`output_format_avro_sync_interval`](/operations/settings/settings-formats.md/#output_format_avro_sync_interval) respectively.

### Example Data {#example-data}

Using the ClickHouse [`DESCRIBE`](/sql-reference/statements/describe-table) function, you can quickly view the inferred format of an Avro file like the following example. 
This example includes the URL of a publicly accessible Avro file in the ClickHouse S3 public bucket:

```sql title="Query"
DESCRIBE url('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits.avro','Avro);
```
```response title="Response"
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

## Format Settings {#format-settings}

| Setting                                     | Description                                                                                         | Default |
|---------------------------------------------|-----------------------------------------------------------------------------------------------------|---------|
| `input_format_avro_allow_missing_fields`    | For Avro/AvroConfluent format: when field is not found in schema use default value instead of error | `0`     |
| `input_format_avro_null_as_default`         | For Avro/AvroConfluent format: insert default in case of null and non Nullable column                 |   `0`   |
| `format_avro_schema_registry_url`           | For AvroConfluent format: Confluent Schema Registry URL.                                            |         |
| `output_format_avro_codec`                  | Compression codec used for output. Possible values: 'null', 'deflate', 'snappy', 'zstd'.            |         |
| `output_format_avro_sync_interval`          | Sync interval in bytes.                                                                             | `16384` |
| `output_format_avro_string_column_pattern`  | For Avro format: regexp of String columns to select as AVRO string.                                 |         |
| `output_format_avro_rows_in_file`           | Max rows in a file (if permitted by storage)                                                         | `1`     |
