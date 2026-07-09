---
alias: []
description: 'CapnProto 相关文档'
input_format: true
keywords: ['CapnProto']
output_format: true
slug: /interfaces/formats/CapnProto
title: 'CapnProto'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 描述
</div>

`CapnProto` 格式是一种二进制消息格式，类似于 [`Protocol Buffers`](https://developers.google.com/protocol-buffers/) 格式和 [Thrift](https://en.wikipedia.org/wiki/Apache_Thrift)，但不同于 [JSON](./JSON/JSON.md) 或 [MessagePack](https://msgpack.org/)。
CapnProto 消息具有严格的类型约束，且不是自描述的，这意味着它们需要外部 schema 描述。schema 会在运行时应用，并针对每个查询进行缓存。

另请参见 [Format Schema](/zh/interfaces/formats/#formatschema)。

<div id="data_types-matching-capnproto">
  ## 数据类型匹配
</div>

下表显示了支持的数据类型，以及它们在 `INSERT` 和 `SELECT` 查询中与 ClickHouse [数据类型](/zh/sql-reference/data-types/index.md) 的对应关系。

| CapnProto 数据类型 (`INSERT`)                            | ClickHouse 数据类型                                                                                                                                        | CapnProto 数据类型 (`SELECT`)                            |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| `UINT8`, `BOOL`                                      | [UInt8](/zh/sql-reference/data-types/int-uint.md)                                                                                                         | `UINT8`                                              |
| `INT8`                                               | [Int8](/zh/sql-reference/data-types/int-uint.md)                                                                                                          | `INT8`                                               |
| `UINT16`                                             | [UInt16](/zh/sql-reference/data-types/int-uint.md), [Date](/zh/sql-reference/data-types/date.md)                                                             | `UINT16`                                             |
| `INT16`                                              | [Int16](/zh/sql-reference/data-types/int-uint.md)                                                                                                         | `INT16`                                              |
| `UINT32`                                             | [UInt32](/zh/sql-reference/data-types/int-uint.md), [DateTime](/zh/sql-reference/data-types/datetime.md)                                                     | `UINT32`                                             |
| `INT32`                                              | [Int32](/zh/sql-reference/data-types/int-uint.md), [Decimal32](/zh/sql-reference/data-types/decimal.md)                                                      | `INT32`                                              |
| `UINT64`                                             | [UInt64](/zh/sql-reference/data-types/int-uint.md)                                                                                                        | `UINT64`                                             |
| `INT64`                                              | [Int64](/zh/sql-reference/data-types/int-uint.md), [DateTime64](/zh/sql-reference/data-types/datetime.md), [Decimal64](/zh/sql-reference/data-types/decimal.md) | `INT64`                                              |
| `FLOAT32`                                            | [Float32](/zh/sql-reference/data-types/float.md)                                                                                                          | `FLOAT32`                                            |
| `FLOAT64`                                            | [Float64](/zh/sql-reference/data-types/float.md)                                                                                                          | `FLOAT64`                                            |
| `TEXT, DATA`                                         | [String](/zh/sql-reference/data-types/string.md), [FixedString](/zh/sql-reference/data-types/fixedstring.md)                                                 | `TEXT, DATA`                                         |
| `union(T, Void), union(Void, T)`                     | [Nullable(T)](/zh/sql-reference/data-types/date.md)                                                                                                       | `union(T, Void), union(Void, T)`                     |
| `ENUM`                                               | [Enum(8/16)](/zh/sql-reference/data-types/enum.md)                                                                                                        | `ENUM`                                               |
| `LIST`                                               | [Array](/zh/sql-reference/data-types/array.md)                                                                                                            | `LIST`                                               |
| `STRUCT`                                             | [Tuple](/zh/sql-reference/data-types/tuple.md)                                                                                                            | `STRUCT`                                             |
| `UINT32`                                             | [IPv4](/zh/sql-reference/data-types/ipv4.md)                                                                                                              | `UINT32`                                             |
| `DATA`                                               | [IPv6](/zh/sql-reference/data-types/ipv6.md)                                                                                                              | `DATA`                                               |
| `DATA`                                               | [Int128/UInt128/Int256/UInt256](/zh/sql-reference/data-types/int-uint.md)                                                                                 | `DATA`                                               |
| `DATA`                                               | [Decimal128/Decimal256](/zh/sql-reference/data-types/decimal.md)                                                                                          | `DATA`                                               |
| `STRUCT(entries LIST(STRUCT(key Key, value Value)))` | [Map](/zh/sql-reference/data-types/map.md)                                                                                                                | `STRUCT(entries LIST(STRUCT(key Key, value Value)))` |

* 整数类型在输入和输出时可以相互转换。
* 如需在 CapnProto 格式中使用 `Enum`，请使用 [format&#95;capn&#95;proto&#95;enum&#95;comparising&#95;mode](/zh/operations/settings/settings-formats.md/#format_capn_proto_enum_comparising_mode) 设置。
* Array 可以嵌套，其参数值也可以是 `Nullable` 类型。`Tuple` 和 `Map` 类型同样可以嵌套。

<div id="example-usage">
  ## 使用示例
</div>

<div id="inserting-and-selecting-data-capnproto">
  ### 插入和查询数据
</div>

你可以使用以下命令，将文件中的 CapnProto 数据插入 ClickHouse 表：

```bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_schema = 'schema:Message' FORMAT CapnProto"
```

`schema.capnp` 内容如下：

```capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

您可以使用以下命令从 ClickHouse 表中查询数据，并将其以 `CapnProto` 格式保存到文件中：

```bash
$ clickhouse-client --query = "SELECT * FROM test.hits FORMAT CapnProto SETTINGS format_schema = 'schema:Message'"
```

<div id="using-autogenerated-capn-proto-schema">
  ### 使用自动生成的 schema
</div>

如果你的数据没有外部 `CapnProto` schema，仍然可以借助自动生成的 schema 以 `CapnProto` 格式导出/导入数据。

例如：

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS format_capn_proto_use_autogenerated_schema=1
```

在这种情况下，ClickHouse 会根据表结构，使用函数 [structureToCapnProtoSchema](/zh/sql-reference/functions/other-functions.md#structureToCapnProtoSchema) 自动生成 CapnProto schema，并使用该 schema 将数据序列化为 CapnProto 格式。

你也可以使用自动生成的 schema 读取 CapnProto 文件 (在这种情况下，该文件必须使用相同的 schema 创建) ：

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_capn_proto_use_autogenerated_schema=1 FORMAT CapnProto"
```

<div id="format-settings">
  ## 格式设置
</div>

设置 [`format_capn_proto_use_autogenerated_schema`](../../operations/settings/settings-formats.md/#format_capn_proto_use_autogenerated_schema) 默认处于启用状态，并且仅在未设置 [`format_schema`](/zh/interfaces/formats#formatschema) 时适用。

你也可以在输入/输出过程中，使用设置 [`output_format_schema`](/zh/operations/settings/formats#output_format_schema) 将自动生成的 schema 保存到文件中。

例如：

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS 
    format_capn_proto_use_autogenerated_schema=1,
    output_format_schema='path/to/schema/schema.capnp'
```

在这种情况下，自动生成的 `CapnProto` schema 会保存在 `path/to/schema/schema.capnp` 文件中。