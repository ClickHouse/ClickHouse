---
sidebar_label: バイナリとネイティブ
slug: /ja/integrations/data-formats/binary-native
---

# ClickHouseでのネイティブおよびバイナリフォーマットの使用

ClickHouseは複数のバイナリフォーマットをサポートしており、これによりパフォーマンスとスペースの効率が向上します。バイナリフォーマットは、データがバイナリ形式で保存されるため、文字エンコーディングの安全性も確保されます。

ここでは、デモンストレーション用に `some_data` [テーブル](assets/some_data.sql)および[データ](assets/some_data.tsv)を使用します。ご自身のClickHouseインスタンスで再現してみてください。

## ネイティブClickHouseフォーマットでのエクスポート

ClickHouseノード間でデータをエクスポートおよびインポートする最も効率的なデータフォーマットは[ネイティブ](/docs/ja/interfaces/formats.md/#native)フォーマットです。エクスポートは `INTO OUTFILE` 句を使用して行われます：

```sql
SELECT * FROM some_data
INTO OUTFILE 'data.clickhouse' FORMAT Native
```

これにより、ネイティブフォーマットの[data.clickhouse](assets/data.clickhouse)ファイルが作成されます。

### ネイティブフォーマットからのインポート

データをインポートするには、小さなファイルや探索目的で[file()](/docs/ja/sql-reference/table-functions/file.md)を使用できます：

```sql
DESCRIBE file('data.clickhouse', Native);
```
```response
┌─name──┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ path  │ String │              │                    │         │                  │                │
│ month │ Date   │              │                    │         │                  │                │
│ hits  │ UInt32 │              │                    │         │                  │                │
└───────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

:::tip
`file()` 関数を使用する場合、ClickHouse Cloudではファイルが存在するマシン上で `clickhouse client` でコマンドを実行する必要があります。他のオプションとして[`clickhouse-local`](/docs/ja/operations/utilities/clickhouse-local.md)を使用してローカルでファイルを探索することができます。
:::

本番環境では、`FROM INFILE` を使用してデータをインポートします：

```sql
INSERT INTO sometable
FROM INFILE 'data.clickhouse'
FORMAT Native
```

### ネイティブフォーマットの圧縮

データをネイティブフォーマットにエクスポートする際（および他のほとんどのフォーマット）に、`COMPRESSION` 句を使用して圧縮を有効にすることもできます：

```sql
SELECT * FROM some_data
INTO OUTFILE 'data.clickhouse'
COMPRESSION 'lz4'
FORMAT Native
```

エクスポートにはLZ4圧縮を使用しました。データをインポートするときにはそれを指定する必要があります：

```sql
INSERT INTO sometable
FROM INFILE 'data.clickhouse'
COMPRESSION 'lz4'
FORMAT Native
```

## RowBinaryへのエクスポート

サポートされているもう一つのバイナリフォーマットは[RowBinary](/docs/ja/interfaces/formats.md/#rowbinary)で、バイナリ表現された行でデータをインポートおよびエクスポートできます：

```sql
SELECT * FROM some_data
INTO OUTFILE 'data.binary' FORMAT RowBinary
```

これにより、バイナリ行形式の[data.binary](assets/data.binary)ファイルが生成されます。

### RowBinaryファイルの探索

このフォーマットでは自動スキーマ推論はサポートされていないので、読み込む前にスキーマを明示的に定義する必要があります：

```sql
SELECT *
FROM file('data.binary', RowBinary, 'path String, month Date, hits UInt32')
LIMIT 5
```
```response
┌─path───────────────────────────┬──────month─┬─hits─┐
│ Bangor_City_Forest             │ 2015-07-01 │   34 │
│ Alireza_Afzal                  │ 2017-02-01 │   24 │
│ Akhaura-Laksam-Chittagong_Line │ 2015-09-01 │   30 │
│ 1973_National_500              │ 2017-10-01 │   80 │
│ Attachment                     │ 2017-09-01 │ 1356 │
└────────────────────────────────┴────────────┴──────┘
```

[RowBinaryWithNames](/docs/ja/interfaces/formats.md/#rowbinarywithnames)を利用すると、カラム一覧を含むヘッダー行も追加されます。[RowBinaryWithNamesAndTypes](/docs/ja/interfaces/formats.md/#rowbinarywithnamesandtypes)では、カラムタイプを含む追加のヘッダー行も追加されます。

### RowBinaryファイルからのインポート

RowBinaryファイルからデータを読み込むには、`FROM INFILE` 句を使用します：

```sql
INSERT INTO sometable
FROM INFILE 'data.binary'
FORMAT RowBinary
```

## RawBLOBを使用した単一バイナリ値のインポート

バイナリファイル全体を読み込み、テーブルのフィールドに保存したい場合があります。このような場合には、RawBLOB形式を使用できます。この形式は単一カラムテーブルでのみ直接使用できます：

```sql
CREATE TABLE images(data String) Engine = Memory
```

画像ファイルを `images` テーブルに保存してみましょう：

```bash
cat image.jpg | clickhouse-client -q "INSERT INTO images FORMAT RawBLOB"
```

`data` フィールドの長さを確認すると、元のファイルサイズと等しくなります：

```sql
SELECT length(data) FROM images
```
```response
┌─length(data)─┐
│         6121 │
└──────────────┘
```

### RawBLOBデータのエクスポート

この形式はまた、`INTO OUTFILE` 句を使用してデータをエクスポートする際にも使用できます：

```sql
SELECT * FROM images LIMIT 1
INTO OUTFILE 'out.jpg'
FORMAT RawBLOB
```

`LIMIT 1`を使用する必要があるのは、複数の値をエクスポートするとファイルが破損するためです。

## MessagePack

ClickHouseは[MessagePack](https://msgpack.org/)を使用して[MsgPack](/docs/ja/interfaces/formats.md/#msgpack)のインポートとエクスポートをサポートしています。MessagePack形式にエクスポートするには：

```sql
SELECT *
FROM some_data
INTO OUTFILE 'data.msgpk'
FORMAT MsgPack
```

[MessagePackファイル](assets/data.msgpk)からデータをインポートするには：

```sql
INSERT INTO sometable
FROM INFILE 'data.msgpk'
FORMAT MsgPack
```

## プロトコルバッファ

[プロトコルバッファ](/docs/ja/interfaces/formats.md/#protobuf)を使用するには、まず[スキーマファイル](assets/schema.proto)を定義する必要があります：

```protobuf
syntax = "proto3";

message MessageType {
  string path = 1;
  date month = 2;
  uint32 hits = 3;
};
```

このスキーマファイルのパス（私たちの場合は`schema.proto`）は、[Protobuf](/docs/ja/interfaces/formats.md/#protobuf)フォーマット用の`format_schema`設定オプションに設定されます：

```sql
SELECT * FROM some_data
INTO OUTFILE 'proto.bin'
FORMAT Protobuf
SETTINGS format_schema = 'schema:MessageType'
```

これにより、データが[proto.bin](assets/proto.bin)ファイルに保存されます。ClickHouseはProtobufデータのインポートやネストされたメッセージもサポートしています。[ProtobufSingle](/docs/ja/interfaces/formats.md/#protobufsingle)を使用して、単一のプロトコルバッファメッセージを扱うことも検討してください（この場合、長さ区切りは省略されます）。

## Cap’n Proto

ClickHouseがサポートするもう一つの人気のあるバイナリシリアライゼーションフォーマットは[Cap’n Proto](https://capnproto.org/)です。`Protobuf`形式と同様に、スキーマファイル([schema.capnp](assets/schema.capnp))を定義する必要があります：

```
@0xec8ff1a10aa10dbe;

struct PathStats {
  path @0 :Text;
  month @1 :UInt32;
  hits @2 :UInt32;
}
```

このスキーマを使用して[CapnProto](/docs/ja/interfaces/formats.md/#capnproto)フォーマットでインポートとエクスポートを行います：

```sql
SELECT
    path,
    CAST(month, 'UInt32') AS month,
    hits
FROM some_data
INTO OUTFILE 'capnp.bin'
FORMAT CapnProto
SETTINGS format_schema = 'schema:PathStats'
```

[`CapnProto`対応の型](/docs/ja/interfaces/formats.md/#data_types-matching-capnproto)に合わせるため、`Date`カラムを`UInt32`としてキャストする必要があることに注意してください。

## その他のフォーマット

ClickHouseでは、さまざまなシナリオやプラットフォームに対応するために、多くのフォーマット、テキストフォーマットとバイナリフォーマットの両方が導入されています。以下の記事で、さらに多くのフォーマットとそれを扱う方法を探索してください：

- [CSVおよびTSVフォーマット](csv-tsv.md)
- [Parquet](parquet.md)
- [JSONフォーマット](/docs/ja/integrations/data-ingestion/data-formats/json/intro.md)
- [正規表現とテンプレート](templates-regex.md)
- **ネイティブおよびバイナリフォーマット**
- [SQLフォーマット](sql.md)

また、[clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)もチェックし、ClickHouseサーバーを起動せずにローカル/リモートファイルで作業するためのポータブルな完全機能のツールです。
