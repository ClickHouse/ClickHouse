---
title : CapnProto
slug : /en/interfaces/formats/CapnProto
keywords : [CapnProto]
---

## Description

CapnProto is a binary message format similar to [Protocol Buffers](https://developers.google.com/protocol-buffers/) and [Thrift](https://en.wikipedia.org/wiki/Apache_Thrift), but not like [JSON](/docs/en/interfaces/formats/JSON) or [MessagePack](https://msgpack.org/).
CapnProto messages are strictly typed and not self-describing, meaning they need an external schema description. The schema is applied on the fly and cached for each query.
See also [Format Schema](/en/interfaces/formats/#formatschema).

## Data Types Matching {#data_types-matching-capnproto}

The table below shows supported data types and how they match ClickHouse [data types](/docs/en/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| CapnProto data type (`INSERT`)                       | ClickHouse data type                                                                                                                                                           | CapnProto data type (`SELECT`)                       |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| `UINT8`, `BOOL`                                      | [UInt8](/docs/en/sql-reference/data-types/int-uint.md)                                                                                                                         | `UINT8`                                              |
| `INT8`                                               | [Int8](/docs/en/sql-reference/data-types/int-uint.md)                                                                                                                          | `INT8`                                               |
| `UINT16`                                             | [UInt16](/docs/en/sql-reference/data-types/int-uint.md), [Date](/docs/en/sql-reference/data-types/date.md)                                                                     | `UINT16`                                             |
| `INT16`                                              | [Int16](/docs/en/sql-reference/data-types/int-uint.md)                                                                                                                         | `INT16`                                              |
| `UINT32`                                             | [UInt32](/docs/en/sql-reference/data-types/int-uint.md), [DateTime](/docs/en/sql-reference/data-types/datetime.md)                                                             | `UINT32`                                             |
| `INT32`                                              | [Int32](/docs/en/sql-reference/data-types/int-uint.md), [Decimal32](/docs/en/sql-reference/data-types/decimal.md)                                                              | `INT32`                                              |
| `UINT64`                                             | [UInt64](/docs/en/sql-reference/data-types/int-uint.md)                                                                                                                        | `UINT64`                                             |
| `INT64`                                              | [Int64](/docs/en/sql-reference/data-types/int-uint.md), [DateTime64](/docs/en/sql-reference/data-types/datetime.md), [Decimal64](/docs/en/sql-reference/data-types/decimal.md) | `INT64`                                              |
| `FLOAT32`                                            | [Float32](/docs/en/sql-reference/data-types/float.md)                                                                                                                          | `FLOAT32`                                            |
| `FLOAT64`                                            | [Float64](/docs/en/sql-reference/data-types/float.md)                                                                                                                          | `FLOAT64`                                            |
| `TEXT, DATA`                                         | [String](/docs/en/sql-reference/data-types/string.md), [FixedString](/docs/en/sql-reference/data-types/fixedstring.md)                                                         | `TEXT, DATA`                                         |
| `union(T, Void), union(Void, T)`                     | [Nullable(T)](/docs/en/sql-reference/data-types/date.md)                                                                                                                       | `union(T, Void), union(Void, T)`                     |
| `ENUM`                                               | [Enum(8/16)](/docs/en/sql-reference/data-types/enum.md)                                                                                                                        | `ENUM`                                               |
| `LIST`                                               | [Array](/docs/en/sql-reference/data-types/array.md)                                                                                                                            | `LIST`                                               |
| `STRUCT`                                             | [Tuple](/docs/en/sql-reference/data-types/tuple.md)                                                                                                                            | `STRUCT`                                             |
| `UINT32`                                             | [IPv4](/docs/en/sql-reference/data-types/ipv4.md)                                                                                                                              | `UINT32`                                             |
| `DATA`                                               | [IPv6](/docs/en/sql-reference/data-types/ipv6.md)                                                                                                                              | `DATA`                                               |
| `DATA`                                               | [Int128/UInt128/Int256/UInt256](/docs/en/sql-reference/data-types/int-uint.md)                                                                                                 | `DATA`                                               |
| `DATA`                                               | [Decimal128/Decimal256](/docs/en/sql-reference/data-types/decimal.md)                                                                                                          | `DATA`                                               |
| `STRUCT(entries LIST(STRUCT(key Key, value Value)))` | [Map](/docs/en/sql-reference/data-types/map.md)                                                                                                                                | `STRUCT(entries LIST(STRUCT(key Key, value Value)))` |

- Integer types can be converted into each other during input/output.
- For working with `Enum` in CapnProto format use the [format_capn_proto_enum_comparising_mode](/docs/en/operations/settings/settings-formats.md/#format_capn_proto_enum_comparising_mode) setting.
- Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.

## Example Usage

### Inserting and Selecting Data {#inserting-and-selecting-data-capnproto}

You can insert CapnProto data from a file into ClickHouse table by the following command:

``` bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_schema = 'schema:Message' FORMAT CapnProto"
```

Where `schema.capnp` looks like this:

``` capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

You can select data from a ClickHouse table and save them into some file in the CapnProto format by the following command:

``` bash
$ clickhouse-client --query = "SELECT * FROM test.hits FORMAT CapnProto SETTINGS format_schema = 'schema:Message'"
```

### Using autogenerated schema {#using-autogenerated-capn-proto-schema}

If you don't have an external CapnProto schema for your data, you can still output/input data in CapnProto format using autogenerated schema.
For example:

```sql
SELECT * FROM test.hits format CapnProto SETTINGS format_capn_proto_use_autogenerated_schema=1
```

In this case ClickHouse will autogenerate CapnProto schema according to the table structure using function [structureToCapnProtoSchema](docs/en/sql-reference/functions/other-functions.md#structure_to_capn_proto_schema) and will use this schema to serialize data in CapnProto format.

You can also read CapnProto file with autogenerated schema (in this case the file must be created using the same schema):

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_capn_proto_use_autogenerated_schema=1 FORMAT CapnProto"
```

The setting [format_capn_proto_use_autogenerated_schema](docs/en/operations/settings/settings-formats.md#format_capn_proto_use_autogenerated_schema) is enabled by default and applies if [format_schema](docs/en/operations/settings/settings-formats.md#formatschema-format-schema) is not set.

You can also save autogenerated schema in the file during input/output using setting [output_format_schema](docs/en/operations/settings/settings-formats.md#outputformatschema-output-format-schema). For example:

```sql
SELECT * FROM test.hits format CapnProto SETTINGS format_capn_proto_use_autogenerated_schema=1, output_format_schema='path/to/schema/schema.capnp'
```

In this case autogenerated CapnProto schema will be saved in file `path/to/schema/schema.capnp`.

## Format Settings