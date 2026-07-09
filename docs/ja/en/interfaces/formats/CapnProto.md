---
alias: []
description: 'CapnProtoに関するドキュメント'
input_format: true
keywords: ['CapnProto']
output_format: true
slug: /interfaces/formats/CapnProto
title: 'CapnProto'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

`CapnProto` フォーマットは、[`Protocol Buffers`](https://developers.google.com/protocol-buffers/) や [Thrift](https://en.wikipedia.org/wiki/Apache_Thrift) に似たバイナリメッセージフォーマットですが、[JSON](./JSON/JSON.md) や [MessagePack](https://msgpack.org/) とは異なります。
CapnProto メッセージは厳密に型付けされており、自己記述型ではないため、外部のスキーマ記述が必要です。スキーマは動的に適用され、各クエリごとにキャッシュされます。

関連項目 [Format Schema](/ja/interfaces/formats/#formatschema)。

<div id="data_types-matching-capnproto">
  ## データ型の対応
</div>

以下の表は、サポートされているデータ型と、`INSERT` および `SELECT` クエリにおいてそれらが ClickHouse の[データ型](/ja/sql-reference/data-types/index.md)にどのように対応するかを示しています。

| CapnProto データ型 (`INSERT`)                            | ClickHouse データ型                                                                                                                                        | CapnProto データ型 (`SELECT`)                            |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| `UINT8`, `BOOL`                                      | [UInt8](/ja/sql-reference/data-types/int-uint.md)                                                                                                         | `UINT8`                                              |
| `INT8`                                               | [Int8](/ja/sql-reference/data-types/int-uint.md)                                                                                                          | `INT8`                                               |
| `UINT16`                                             | [UInt16](/ja/sql-reference/data-types/int-uint.md), [Date](/ja/sql-reference/data-types/date.md)                                                             | `UINT16`                                             |
| `INT16`                                              | [Int16](/ja/sql-reference/data-types/int-uint.md)                                                                                                         | `INT16`                                              |
| `UINT32`                                             | [UInt32](/ja/sql-reference/data-types/int-uint.md), [DateTime](/ja/sql-reference/data-types/datetime.md)                                                     | `UINT32`                                             |
| `INT32`                                              | [Int32](/ja/sql-reference/data-types/int-uint.md), [Decimal32](/ja/sql-reference/data-types/decimal.md)                                                      | `INT32`                                              |
| `UINT64`                                             | [UInt64](/ja/sql-reference/data-types/int-uint.md)                                                                                                        | `UINT64`                                             |
| `INT64`                                              | [Int64](/ja/sql-reference/data-types/int-uint.md), [DateTime64](/ja/sql-reference/data-types/datetime.md), [Decimal64](/ja/sql-reference/data-types/decimal.md) | `INT64`                                              |
| `FLOAT32`                                            | [Float32](/ja/sql-reference/data-types/float.md)                                                                                                          | `FLOAT32`                                            |
| `FLOAT64`                                            | [Float64](/ja/sql-reference/data-types/float.md)                                                                                                          | `FLOAT64`                                            |
| `TEXT, DATA`                                         | [String](/ja/sql-reference/data-types/string.md), [FixedString](/ja/sql-reference/data-types/fixedstring.md)                                                 | `TEXT, DATA`                                         |
| `union(T, Void), union(Void, T)`                     | [Nullable(T)](/ja/sql-reference/data-types/date.md)                                                                                                       | `union(T, Void), union(Void, T)`                     |
| `ENUM`                                               | [Enum(8/16)](/ja/sql-reference/data-types/enum.md)                                                                                                        | `ENUM`                                               |
| `LIST`                                               | [Array](/ja/sql-reference/data-types/array.md)                                                                                                            | `LIST`                                               |
| `STRUCT`                                             | [Tuple](/ja/sql-reference/data-types/tuple.md)                                                                                                            | `STRUCT`                                             |
| `UINT32`                                             | [IPv4](/ja/sql-reference/data-types/ipv4.md)                                                                                                              | `UINT32`                                             |
| `DATA`                                               | [IPv6](/ja/sql-reference/data-types/ipv6.md)                                                                                                              | `DATA`                                               |
| `DATA`                                               | [Int128/UInt128/Int256/UInt256](/ja/sql-reference/data-types/int-uint.md)                                                                                 | `DATA`                                               |
| `DATA`                                               | [Decimal128/Decimal256](/ja/sql-reference/data-types/decimal.md)                                                                                          | `DATA`                                               |
| `STRUCT(entries LIST(STRUCT(key Key, value Value)))` | [Map](/ja/sql-reference/data-types/map.md)                                                                                                                | `STRUCT(entries LIST(STRUCT(key Key, value Value)))` |

* 整数型は、入出力時に相互変換できます。
* CapnProto フォーマットで `Enum` を扱う場合は、[format&#95;capn&#95;proto&#95;enum&#95;comparising&#95;mode](/ja/operations/settings/settings-formats.md/#format_capn_proto_enum_comparising_mode) 設定を使用してください。
* Array はネストでき、引数に `Nullable` 型を取ることもできます。`Tuple` 型と `Map` 型もネストできます。

<div id="example-usage">
  ## 使用例
</div>

<div id="inserting-and-selecting-data-capnproto">
  ### データの挿入と取得
</div>

次のコマンドを使用すると、ファイルからCapnProtoデータをClickHouseテーブルに挿入できます。

```bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_schema = 'schema:Message' FORMAT CapnProto"
```

`schema.capnp` の内容は次のようになります:

```capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

次のコマンドを使用すると、ClickHouseテーブルからデータを取得し、`CapnProto` フォーマットのファイルに保存できます。

```bash
$ clickhouse-client --query = "SELECT * FROM test.hits FORMAT CapnProto SETTINGS format_schema = 'schema:Message'"
```

<div id="using-autogenerated-capn-proto-schema">
  ### 自動生成スキーマの使用
</div>

データ用の外部 `CapnProto` スキーマがない場合でも、自動生成スキーマを使って `CapnProto` フォーマットでデータを出力/入力できます。

例えば:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS format_capn_proto_use_autogenerated_schema=1
```

この場合、ClickHouse は関数 [structureToCapnProtoSchema](/ja/sql-reference/functions/other-functions.md#structureToCapnProtoSchema) を使用してテーブル構造に基づく CapnProto スキーマを自動生成し、そのスキーマを使ってデータを CapnProto フォーマットにシリアライズします。

自動生成されたスキーマを使って CapnProto ファイルを読み込むこともできます (この場合、ファイルは同じスキーマを使って作成されている必要があります) :

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_capn_proto_use_autogenerated_schema=1 FORMAT CapnProto"
```

<div id="format-settings">
  ## フォーマット設定
</div>

設定 [`format_capn_proto_use_autogenerated_schema`](../../operations/settings/settings-formats.md/#format_capn_proto_use_autogenerated_schema) はデフォルトで有効になっており、[`format_schema`](/ja/interfaces/formats#formatschema) が設定されていない場合に適用されます。

また、入出力時に設定 [`output_format_schema`](/ja/operations/settings/formats#output_format_schema) を使用して、自動生成スキーマをファイルに保存することもできます。

例:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS 
    format_capn_proto_use_autogenerated_schema=1,
    output_format_schema='path/to/schema/schema.capnp'
```

この場合、自動生成`CapnProto`スキーマは`path/to/schema/schema.capnp`ファイルに保存されます。