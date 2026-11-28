---
alias: []
description: 'Documentation for Capnproto'
input_format: true
keywords: ['CapnProto']
output_format: true
slug: /interfaces/formats/CapnProto
title: 'CapnProto'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge/>

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `CapnProto` format is a binary message format similar to the [`Protocol Buffers`](https://developers.google.com/protocol-buffers/) format and [Thrift](https://en.wikipedia.org/wiki/Apache_Thrift), but not like [JSON](./JSON/JSON.md) or [MessagePack](https://msgpack.org/).
CapnProto messages are strictly typed and not self-describing, meaning they need an external schema description. The schema is applied on the fly and cached for each query.

See also [Format Schema](/interfaces/formats/#formatschema).

## Data types matching {#data_types-matching-capnproto}

The table below shows supported data types and how they match ClickHouse [data types](/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| CapnProto data type (`INSERT`)                       | ClickHouse data type                                                                                                                                                           | CapnProto data type (`SELECT`)                       |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| `UINT8`, `BOOL`                                      | [UInt8](/sql-reference/data-types/int-uint.md)                                                                                                                         | `UINT8`                                              |
| `INT8`                                               | [Int8](/sql-reference/data-types/int-uint.md)                                                                                                                          | `INT8`                                               |
| `UINT16`                                             | [UInt16](/sql-reference/data-types/int-uint.md), [Date](/sql-reference/data-types/date.md)                                                                     | `UINT16`                                             |
| `INT16`                                              | [Int16](/sql-reference/data-types/int-uint.md)                                                                                                                         | `INT16`                                              |
| `UINT32`                                             | [UInt32](/sql-reference/data-types/int-uint.md), [DateTime](/sql-reference/data-types/datetime.md)                                                             | `UINT32`                                             |
| `INT32`                                              | [Int32](/sql-reference/data-types/int-uint.md), [Decimal32](/sql-reference/data-types/decimal.md)                                                              | `INT32`                                              |
| `UINT64`                                             | [UInt64](/sql-reference/data-types/int-uint.md)                                                                                                                        | `UINT64`                                             |
| `INT64`                                              | [Int64](/sql-reference/data-types/int-uint.md), [DateTime64](/sql-reference/data-types/datetime.md), [Decimal64](/sql-reference/data-types/decimal.md) | `INT64`                                              |
| `FLOAT32`                                            | [Float32](/sql-reference/data-types/float.md)                                                                                                                          | `FLOAT32`                                            |
| `FLOAT64`                                            | [Float64](/sql-reference/data-types/float.md)                                                                                                                          | `FLOAT64`                                            |
| `TEXT, DATA`                                         | [String](/sql-reference/data-types/string.md), [FixedString](/sql-reference/data-types/fixedstring.md)                                                         | `TEXT, DATA`                                         |
| `union(T, Void), union(Void, T)`                     | [Nullable(T)](/sql-reference/data-types/date.md)                                                                                                                       | `union(T, Void), union(Void, T)`                     |
| `ENUM`                                               | [Enum(8/16)](/sql-reference/data-types/enum.md)                                                                                                                        | `ENUM`                                               |
| `LIST`                                               | [Array](/sql-reference/data-types/array.md)                                                                                                                            | `LIST`                                               |
| `STRUCT`                                             | [Tuple](/sql-reference/data-types/tuple.md)                                                                                                                            | `STRUCT`                                             |
| `UINT32`                                             | [IPv4](/sql-reference/data-types/ipv4.md)                                                                                                                              | `UINT32`                                             |
| `DATA`                                               | [IPv6](/sql-reference/data-types/ipv6.md)                                                                                                                              | `DATA`                                               |
| `DATA`                                               | [Int128/UInt128/Int256/UInt256](/sql-reference/data-types/int-uint.md)                                                                                                 | `DATA`                                               |
| `DATA`                                               | [Decimal128/Decimal256](/sql-reference/data-types/decimal.md)                                                                                                          | `DATA`                                               |
| `STRUCT(entries LIST(STRUCT(key Key, value Value)))` | [Map](/sql-reference/data-types/map.md)                                                                                                                                | `STRUCT(entries LIST(STRUCT(key Key, value Value)))` |

- Integer types can be converted into each other during input/output.
- For working with `Enum` in CapnProto format use the [format_capn_proto_enum_comparising_mode](/operations/settings/settings-formats.md/#format_capn_proto_enum_comparising_mode) setting.
- Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.

## Example usage {#example-usage}

### Inserting and selecting data {#inserting-and-selecting-data-capnproto}

You can insert CapnProto data from a file into ClickHouse table by the following command:

```bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_schema = 'schema:Message' FORMAT CapnProto"
```

Where the `schema.capnp` looks like this:

```capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

You can select data from a ClickHouse table and save them into some file in the `CapnProto` format using the following command:

```bash
$ clickhouse-client --query = "SELECT * FROM test.hits FORMAT CapnProto SETTINGS format_schema = 'schema:Message'"
```

### Using autogenerated schema {#using-autogenerated-capn-proto-schema}

If you don't have an external `CapnProto` schema for your data, you can still output/input data in `CapnProto` format using autogenerated schema.

For example:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS format_capn_proto_use_autogenerated_schema=1
```

In this case, ClickHouse will autogenerate CapnProto schema according to the table structure using function [structureToCapnProtoSchema](/sql-reference/functions/other-functions.md#structureToCapnProtoSchema) and will use this schema to serialize data in CapnProto format.

You can also read CapnProto file with autogenerated schema (in this case the file must be created using the same schema):

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_capn_proto_use_autogenerated_schema=1 FORMAT CapnProto"
```

## Format settings {#format-settings}

The setting [`format_capn_proto_use_autogenerated_schema`](../../operations/settings/settings-formats.md/#format_capn_proto_use_autogenerated_schema) is enabled by default and is applicable if [`format_schema`](/interfaces/formats#formatschema) is not set.

You can also save the autogenerated schema to a file during input/output using setting [`output_format_schema`](/operations/settings/formats#output_format_schema). 

For example:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS 
    format_capn_proto_use_autogenerated_schema=1,
    output_format_schema='path/to/schema/schema.capnp'
```
In this case, the autogenerated `CapnProto` schema will be saved in file `path/to/schema/schema.capnp`.