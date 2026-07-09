---
description: 'ClickHouseでサポートされている入出力データフォーマットの概要'
sidebar_label: 'すべてのフォーマットを表示...'
sidebar_position: 21
slug: /interfaces/formats
title: '入出力データのフォーマット'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="formats-for-input-and-output-data">
  # 入出力データのフォーマット
</div>

ClickHouse は、一般的なテキストおよびバイナリデータのフォーマットのほとんどをサポートしています。これにより、ほぼあらゆる
データパイプラインに簡単に組み込み、ClickHouse の利点を活用できます。

<div id="input-formats">
  ## 入力フォーマット
</div>

入力フォーマットは、次の用途で使用されます。

* `INSERT` ステートメントに渡されるデータのパース
* `File`、`URL`、`HDFS` などのファイルベースのテーブルに対する `SELECT` クエリの実行
* 辞書の読み取り

ClickHouse で効率的にデータをインジェストするには、適切な入力フォーマットを選ぶことが重要です。対応フォーマットは 70 種類を超えており、
最も高性能なものを選ぶことで、挿入速度、CPU とメモリ使用量、そしてシステム全体の
効率に大きな影響を与える可能性があります。こうした選択をしやすくするために、私たちはフォーマットごとのインジェスト性能をベンチマークし、次の重要な知見を得ました。

* **[Native](formats/Native.md) フォーマットは最も効率的な入力フォーマットです**。最良の圧縮率、最小の
  リソース使用量、そして最小限のサーバー側の処理オーバーヘッドを実現します。
* **圧縮は不可欠です** - LZ4 は CPU コストをほとんど増やさずにデータサイズを削減し、ZSTD は
  追加の CPU 使用量と引き換えに、より高い圧縮率を提供します。
* **事前ソートの影響は中程度です**。ClickHouse はすでに効率的にソートを行うためです。
* **バッチ処理は効率を大幅に向上させます** - バッチを大きくすると挿入のオーバーヘッドが減り、スループットが向上します。

結果の詳細な分析とベストプラクティスについては、
完全版の [benchmark analysis](https://www.clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient) をお読みください。
テスト結果の全体については、[FastFormats](https://fastformats.clickhouse.com/) のオンラインダッシュボードで確認できます。

<div id="output-formats">
  ## 出力フォーマット
</div>

出力に対応するフォーマットは、次の用途で使用されます。

* `SELECT` クエリの結果の整形
* ファイルベースのテーブルへの `INSERT` 操作の実行

<div id="formats-overview">
  ## フォーマットの概要
</div>

対応フォーマットは次のとおりです。

| フォーマット                                                                                                     | 入力 | 出力 |
| ---------------------------------------------------------------------------------------------------------- | -- | -- |
| [TabSeparated](./formats/TabSeparated/TabSeparated.md)                                                     | ✔  | ✔  |
| [TabSeparatedRaw](./formats/TabSeparated/TabSeparatedRaw.md)                                               | ✔  | ✔  |
| [TabSeparatedWithNames](./formats/TabSeparated/TabSeparatedWithNames.md)                                   | ✔  | ✔  |
| [TabSeparatedWithNamesAndTypes](./formats/TabSeparated/TabSeparatedWithNamesAndTypes.md)                   | ✔  | ✔  |
| [TabSeparatedRawWithNames](./formats/TabSeparated/TabSeparatedRawWithNames.md)                             | ✔  | ✔  |
| [TabSeparatedRawWithNamesAndTypes](./formats/TabSeparated/TabSeparatedRawWithNamesAndTypes.md)             | ✔  | ✔  |
| [Template](./formats/Template/Template.md)                                                                 | ✔  | ✔  |
| [TemplateIgnoreSpaces](./formats/Template/TemplateIgnoreSpaces.md)                                         | ✔  | ✗  |
| [CSV](./formats/CSV/CSV.md)                                                                                | ✔  | ✔  |
| [CSVWithNames](./formats/CSV/CSVWithNames.md)                                                              | ✔  | ✔  |
| [CSVWithNamesAndTypes](./formats/CSV/CSVWithNamesAndTypes.md)                                              | ✔  | ✔  |
| [CustomSeparated](./formats/CustomSeparated/CustomSeparated.md)                                            | ✔  | ✔  |
| [CustomSeparatedWithNames](./formats/CustomSeparated/CustomSeparatedWithNames.md)                          | ✔  | ✔  |
| [CustomSeparatedWithNamesAndTypes](./formats/CustomSeparated/CustomSeparatedWithNamesAndTypes.md)          | ✔  | ✔  |
| [SQLInsert](./formats/SQLInsert.md)                                                                        | ✗  | ✔  |
| [Values](./formats/Values.md)                                                                              | ✔  | ✔  |
| [Vertical](./formats/Vertical.md)                                                                          | ✗  | ✔  |
| [JSON](./formats/JSON/JSON.md)                                                                             | ✔  | ✔  |
| [JSONAsString](./formats/JSON/JSONAsString.md)                                                             | ✔  | ✗  |
| [JSONAsObject](./formats/JSON/JSONAsObject.md)                                                             | ✔  | ✗  |
| [JSONStrings](./formats/JSON/JSONStrings.md)                                                               | ✗  | ✔  |
| [JSONColumns](./formats/JSON/JSONColumns.md)                                                               | ✔  | ✔  |
| [JSONColumnsWithMetadata](./formats/JSON/JSONColumnsWithMetadata.md)                                       | ✔  | ✔  |
| [JSONCompact](./formats/JSON/JSONCompact.md)                                                               | ✔  | ✔  |
| [JSONCompactStrings](./formats/JSON/JSONCompactStrings.md)                                                 | ✗  | ✔  |
| [JSONCompactColumns](./formats/JSON/JSONCompactColumns.md)                                                 | ✔  | ✔  |
| [JSONEachRow](./formats/JSON/JSONEachRow.md)                                                               | ✔  | ✔  |
| [PrettyJSONEachRow](./formats/JSON/PrettyJSONEachRow.md)                                                   | ✗  | ✔  |
| [JSONEachRowWithProgress](./formats/JSON/JSONEachRowWithProgress.md)                                       | ✗  | ✔  |
| [JSONStringsEachRow](./formats/JSON/JSONStringsEachRow.md)                                                 | ✔  | ✔  |
| [JSONStringsEachRowWithProgress](./formats/JSON/JSONStringsEachRowWithProgress.md)                         | ✗  | ✔  |
| [JSONCompactEachRow](./formats/JSON/JSONCompactEachRow.md)                                                 | ✔  | ✔  |
| [JSONCompactEachRowWithNames](./formats/JSON/JSONCompactEachRowWithNames.md)                               | ✔  | ✔  |
| [JSONCompactEachRowWithNamesAndTypes](./formats/JSON/JSONCompactEachRowWithNamesAndTypes.md)               | ✔  | ✔  |
| [JSONCompactEachRowWithProgress](./formats/JSON/JSONCompactEachRowWithProgress.md)                         | ✗  | ✔  |
| [JSONCompactStringsEachRow](./formats/JSON/JSONCompactStringsEachRow.md)                                   | ✔  | ✔  |
| [JSONCompactStringsEachRowWithNames](./formats/JSON/JSONCompactStringsEachRowWithNames.md)                 | ✔  | ✔  |
| [JSONCompactStringsEachRowWithNamesAndTypes](./formats/JSON/JSONCompactStringsEachRowWithNamesAndTypes.md) | ✔  | ✔  |
| [JSONCompactStringsEachRowWithProgress](./formats/JSON/JSONCompactStringsEachRowWithProgress.md)           | ✗  | ✔  |
| [JSONObjectEachRow](./formats/JSON/JSONObjectEachRow.md)                                                   | ✔  | ✔  |
| [BSONEachRow](./formats/BSONEachRow.md)                                                                    | ✔  | ✔  |
| [TSKV](./formats/TabSeparated/TSKV.md)                                                                     | ✔  | ✔  |
| [Pretty](./formats/Pretty/Pretty.md)                                                                       | ✗  | ✔  |
| [PrettyNoEscapes](./formats/Pretty/PrettyNoEscapes.md)                                                     | ✗  | ✔  |
| [PrettyMonoBlock](./formats/Pretty/PrettyMonoBlock.md)                                                     | ✗  | ✔  |
| [PrettyNoEscapesMonoBlock](./formats/Pretty/PrettyNoEscapesMonoBlock.md)                                   | ✗  | ✔  |
| [PrettyCompact](./formats/Pretty/PrettyCompact.md)                                                         | ✗  | ✔  |
| [PrettyCompactNoEscapes](./formats/Pretty/PrettyCompactNoEscapes.md)                                       | ✗  | ✔  |
| [PrettyCompactMonoBlock](./formats/Pretty/PrettyCompactMonoBlock.md)                                       | ✗  | ✔  |
| [PrettyCompactNoEscapesMonoBlock](./formats/Pretty/PrettyCompactNoEscapesMonoBlock.md)                     | ✗  | ✔  |
| [PrettySpace](./formats/Pretty/PrettySpace.md)                                                             | ✗  | ✔  |
| [PrettySpaceNoEscapes](./formats/Pretty/PrettySpaceNoEscapes.md)                                           | ✗  | ✔  |
| [PrettySpaceMonoBlock](./formats/Pretty/PrettySpaceMonoBlock.md)                                           | ✗  | ✔  |
| [PrettySpaceNoEscapesMonoBlock](./formats/Pretty/PrettySpaceNoEscapesMonoBlock.md)                         | ✗  | ✔  |
| [Prometheus](./formats/Prometheus.md)                                                                      | ✗  | ✔  |
| [Protobuf](./formats/Protobuf/Protobuf.md)                                                                 | ✔  | ✔  |
| [ProtobufSingle](./formats/Protobuf/ProtobufSingle.md)                                                     | ✔  | ✔  |
| [ProtobufList](./formats/Protobuf/ProtobufList.md)                                                         | ✔  | ✔  |
| [Avro](./formats/Avro/Avro.md)                                                                             | ✔  | ✔  |
| [AvroConfluent](./formats/Avro/AvroConfluent.md)                                                           | ✔  | ✔  |
| [Parquet](./formats/Parquet/Parquet.md)                                                                    | ✔  | ✔  |
| [ParquetMetadata](./formats/Parquet/ParquetMetadata.md)                                                    | ✔  | ✗  |
| [Arrow](./formats/Arrow/Arrow.md)                                                                          | ✔  | ✔  |
| [ArrowStream](./formats/Arrow/ArrowStream.md)                                                              | ✔  | ✔  |
| [ORC](./formats/ORC.md)                                                                                    | ✔  | ✔  |
| [One](./formats/One.md)                                                                                    | ✔  | ✗  |
| [Npy](./formats/Npy.md)                                                                                    | ✔  | ✔  |
| [RowBinary](./formats/RowBinary/RowBinary.md)                                                              | ✔  | ✔  |
| [RowBinaryWithNames](./formats/RowBinary/RowBinaryWithNames.md)                                            | ✔  | ✔  |
| [RowBinaryWithNamesAndTypes](./formats/RowBinary/RowBinaryWithNamesAndTypes.md)                            | ✔  | ✔  |
| [RowBinaryWithDefaults](./formats/RowBinary/RowBinaryWithDefaults.md)                                      | ✔  | ✗  |
| [RowBinaryWithNamesAndTypesAndDefaults](./formats/RowBinary/RowBinaryWithNamesAndTypesAndDefaults.md)      | ✔  | ✗  |
| [Native](./formats/Native.md)                                                                              | ✔  | ✔  |
| [Buffers](./formats/Buffers.md)                                                                            | ✔  | ✔  |
| [Null](./formats/Null.md)                                                                                  | ✗  | ✔  |
| [Hash](./formats/Hash.md)                                                                                  | ✗  | ✔  |
| [XML](./formats/XML.md)                                                                                    | ✗  | ✔  |
| [CapnProto](./formats/CapnProto.md)                                                                        | ✔  | ✔  |
| [LineAsString](./formats/LineAsString/LineAsString.md)                                                     | ✔  | ✔  |
| [LineAsStringWithNames](./formats/LineAsString/LineAsStringWithNames.md)                                   | ✗  | ✔  |
| [LineAsStringWithNamesAndTypes](./formats/LineAsString/LineAsStringWithNamesAndTypes.md)                   | ✗  | ✔  |
| [Regexp](./formats/Regexp.md)                                                                              | ✔  | ✗  |
| [RawBLOB](./formats/RawBLOB.md)                                                                            | ✔  | ✔  |
| [MsgPack](./formats/MsgPack.md)                                                                            | ✔  | ✔  |
| [MySQLDump](./formats/MySQLDump.md)                                                                        | ✔  | ✗  |
| [GeoJSON](./formats/GeoJSON.md)                                                                            | ✔  | ✔  |
| [DWARF](./formats/DWARF.md)                                                                                | ✔  | ✗  |
| [Markdown](./formats/Markdown.md)                                                                          | ✗  | ✔  |
| [Form](./formats/Form.md)                                                                                  | ✔  | ✗  |

ClickHouse の設定で、一部のフォーマット処理パラメータを制御できます。詳しくは、[設定](/ja/operations/settings/settings-formats.md) セクションを参照してください。

<div id="formatschema">
  ## フォーマットスキーマ
</div>

フォーマットスキーマを含むファイル名は、設定 `format_schema` で指定します。
この設定は、`Cap'n Proto` または `Protobuf` フォーマットを使用する場合に必要です。
フォーマットスキーマは、ファイル名とそのファイル内のメッセージ型名をコロンで区切って組み合わせたものです。
たとえば、`schemafile.proto:MessageType` のようになります。
ファイルがそのフォーマットの標準的な拡張子 (たとえば `Protobuf` の `.proto`) を持つ場合、
その拡張子は省略でき、その場合のフォーマットスキーマは `schemafile:MessageType` のようになります。

[client](/ja/interfaces/client.md) を対話型モードで使用してデータを入出力する場合、フォーマットスキーマで指定するファイル名には、
絶対パスまたは client 上の現在のディレクトリからの相対パスを指定できます。
[batch mode](/ja/interfaces/client.md/#batch-mode) で client を使用する場合、セキュリティ上の理由により、スキーマへのパスは相対パスである必要があります。

[HTTP interface](/ja/interfaces/http) を介してデータを入出力する場合、フォーマットスキーマで指定するファイル名は、
サーバー設定の [format&#95;schema&#95;path](/ja/operations/server-configuration-parameters/settings.md/#format_schema_path)
で指定されたディレクトリ内に配置されている必要があります。

<div id="skippingerrors">
  ## エラーのスキップ
</div>

`CSV`、`TabSeparated`、`TSKV`、`JSONEachRow`、`Template`、`CustomSeparated`、`Protobuf` などの一部のフォーマットでは、パースエラーが発生しても、不正な行をスキップして次の行の先頭からパースを続行できます。[input&#95;format&#95;allow&#95;errors&#95;num](/ja/operations/settings/settings-formats.md/#input_format_allow_errors_num) および
[input&#95;format&#95;allow&#95;errors&#95;ratio](/ja/operations/settings/settings-formats.md/#input_format_allow_errors_ratio) 設定を参照してください。
制限事項:

* `JSONEachRow` では、パースエラーが発生すると改行 (または EOF) までのすべてのデータをスキップするため、エラーを正しくカウントするには、行が `\n` で区切られている必要があります。
* `Template` と `CustomSeparated` では、次の行の先頭を見つけるために、最後のカラムの後の区切り文字と行間の区切り文字を使用します。そのため、エラーのスキップが機能するのは、その少なくとも一方が空でない場合に限られます。