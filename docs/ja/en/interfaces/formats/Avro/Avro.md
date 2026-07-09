---
alias: []
description: 'Avroフォーマットに関するドキュメント'
input_format: true
keywords: ['Avro']
output_format: true
slug: /interfaces/formats/Avro
title: 'Avro'
doc_type: 'reference'
---

import DataTypeMapping from './_snippets/data-types-matching.md'

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

[Apache Avro](https://avro.apache.org/) は、効率的なデータ処理のためにバイナリエンコーディングを使用する行指向のシリアライゼーションフォーマットです。`Avro` フォーマットは、[Avroデータファイル](https://avro.apache.org/docs/current/specification/#object-container-files) の読み取りと書き込みをサポートします。このフォーマットでは、埋め込みスキーマを含む自己記述型のメッセージを想定しています。Avro をスキーマレジストリとともに使用する場合は、[`AvroConfluent`](./AvroConfluent.md) フォーマットを参照してください。

<div id="data-type-mapping">
  ## データ型マッピング
</div>

<DataTypeMapping />

<div id="format-settings">
  ## フォーマット設定
</div>

| 設定                                         | 説明                                                                                                              | デフォルト   |
| ------------------------------------------ | --------------------------------------------------------------------------------------------------------------- | ------- |
| `input_format_avro_allow_missing_fields`   | フィールドがスキーマ内に見つからない場合に、エラーを返す代わりにデフォルト値を使用するかどうか。                                                                | `0`     |
| `input_format_avro_null_as_default`        | `null` 値を NULL を許容しないカラムに挿入する際に、エラーを返す代わりにデフォルト値を使用するかどうか。                                                      | `0`     |
| `output_format_avro_codec`                 | Avro 出力ファイルの圧縮アルゴリズム。設定可能な値: `null`, `deflate`, `snappy`, `zstd`。                                               |         |
| `output_format_avro_sync_interval`         | Avro ファイル内の同期マーカーの出現間隔 (バイト単位) 。                                                                                | `16384` |
| `output_format_avro_string_column_pattern` | Avro の文字列型への型マッピングに使用する `String` カラムを識別するための正規表現。デフォルトでは、ClickHouse の `String` カラムは Avro の `bytes` 型として書き込まれます。 |         |
| `output_format_avro_rows_in_file`          | Avro 出力ファイルごとの最大行数。この上限に達すると、新しいファイルが作成されます (ストレージシステムがファイルの分割をサポートしている場合) 。                                    | `1`     |

<div id="examples">
  ## 例
</div>

<div id="reading-avro-data">
  ### Avroデータの読み込み
</div>

AvroファイルからClickHouseテーブルへデータを読み込むには:

```bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

取り込まれる Avro ファイルのルートスキーマは、型 `record` である必要があります。

テーブルのカラムと Avro スキーマのフィールドの対応関係を特定するために、ClickHouse はそれぞれの名前を比較します。
この比較では大文字と小文字が区別され、未使用のフィールドはスキップされます。

ClickHouse テーブルのカラムのデータ型は、挿入される Avro データ内の対応するフィールドと異なる場合があります。データの挿入時には、ClickHouse は上の表に従ってデータ型を解釈し、その後、対応するカラム型にデータを [casts](/ja/sql-reference/functions/type-conversion-functions#CAST) します。

データのインポート時に、スキーマ内にフィールドが見つからず、かつ設定 [`input_format_avro_allow_missing_fields`](/ja/operations/settings/settings-formats.md/#input_format_avro_allow_missing_fields) が有効になっている場合は、エラーをスローする代わりにデフォルト値が使用されます。

<div id="writing-avro-data">
  ### Avroデータの書き込み
</div>

ClickHouseテーブルからAvroファイルにデータを書き込むには：

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

カラム名は次の条件を満たす必要があります。

* `[A-Za-z_]` で始まること
* 使用できる文字は、続けて `[A-Za-z0-9_]` のみ

Avro ファイルの出力圧縮と同期間隔は、それぞれ [`output_format_avro_codec`](/ja/operations/settings/settings-formats.md/#output_format_avro_codec) および [`output_format_avro_sync_interval`](/ja/operations/settings/settings-formats.md/#output_format_avro_sync_interval) 設定で構成できます。

<div id="inferring-the-avro-schema">
  ### Avroスキーマの推論
</div>

ClickHouse の [`DESCRIBE`](/ja/sql-reference/statements/describe-table) 関数を使うと、次の例のような Avro ファイルについて、推論されたフォーマットをすばやく確認できます。
この例には、ClickHouse の S3 Public バケットで公開されている Avro ファイルの URL が含まれています。

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