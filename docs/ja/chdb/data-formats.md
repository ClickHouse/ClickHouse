---
title: データ形式
sidebar_label: データ形式
slug: /ja/chdb/data-formats
description: chDBのデータ形式
keywords: [chdb, データ形式]
---

データ形式について言えば、chDBはClickHouseと100%機能互換です。

入力形式は、`File`、`URL`、または`S3`のようなファイル対応テーブルへの`INSERT`および`SELECT`に提供されるデータを解析するために使用されます。出力形式は、`SELECT`の結果を整理し、ファイル対応テーブルへの`INSERT`を実行するために使用されます。ClickHouseがサポートするデータ形式に加えて、chDBも以下をサポートしています：

- 出力形式としての`ArrowTable`、タイプはPythonの`pyarrow.Table`
- 入力および出力形式としての`DataFrame`、タイプはPythonの`pandas.DataFrame`。例については、[test_joindf.py](https://github.com/chdb-io/chdb/blob/main/tests/test_joindf.py)をご参照ください。
- 出力としての`Debug`（`CSV`のエイリアスとして）、ただしClickHouseからのデバッグ冗長出力を有効にしています。

ClickHouseがサポートするデータ形式は以下の通りです：

| Format                          | Input | Output |
|---------------------------------|-------|--------|
| TabSeparated                    | ✔     | ✔      |
| TabSeparatedRaw                 | ✔     | ✔      |
| TabSeparatedWithNames           | ✔     | ✔      |
| TabSeparatedWithNamesAndTypes   | ✔     | ✔      |
| TabSeparatedRawWithNames        | ✔     | ✔      |
| TabSeparatedRawWithNamesAndTypes| ✔     | ✔      |
| Template                        | ✔     | ✔      |
| TemplateIgnoreSpaces            | ✔     | ✗      |
| CSV                             | ✔     | ✔      |
| CSVWithNames                    | ✔     | ✔      |
| CSVWithNamesAndTypes            | ✔     | ✔      |
| CustomSeparated                 | ✔     | ✔      |
| CustomSeparatedWithNames        | ✔     | ✔      |
| CustomSeparatedWithNamesAndTypes| ✔     | ✔      |
| SQLInsert                       | ✗     | ✔      |
| Values                          | ✔     | ✔      |
| Vertical                        | ✗     | ✔      |
| JSON                            | ✔     | ✔      |
| JSONAsString                    | ✔     | ✗      |
| JSONStrings                     | ✔     | ✔      |
| JSONColumns                     | ✔     | ✔      |
| JSONColumnsWithMetadata         | ✔     | ✔      |
| JSONCompact                     | ✔     | ✔      |
| JSONCompactStrings              | ✗     | ✔      |
| JSONCompactColumns              | ✔     | ✔      |
| JSONEachRow                     | ✔     | ✔      |
| PrettyJSONEachRow               | ✗     | ✔      |
| JSONEachRowWithProgress         | ✗     | ✔      |
| JSONStringsEachRow              | ✔     | ✔      |
| JSONStringsEachRowWithProgress  | ✗     | ✔      |
| JSONCompactEachRow              | ✔     | ✔      |
| JSONCompactEachRowWithNames     | ✔     | ✔      |
| JSONCompactEachRowWithNamesAndTypes | ✔  | ✔      |
| JSONCompactStringsEachRow       | ✔     | ✔      |
| JSONCompactStringsEachRowWithNames | ✔  | ✔      |
| JSONCompactStringsEachRowWithNamesAndTypes | ✔ | ✔ |
| JSONObjectEachRow               | ✔     | ✔      |
| BSONEachRow                     | ✔     | ✔      |
| TSKV                            | ✔     | ✔      |
| Pretty                          | ✗     | ✔      |
| PrettyNoEscapes                 | ✗     | ✔      |
| PrettyMonoBlock                 | ✗     | ✔      |
| PrettyNoEscapesMonoBlock        | ✗     | ✔      |
| PrettyCompact                   | ✗     | ✔      |
| PrettyCompactNoEscapes          | ✗     | ✔      |
| PrettyCompactMonoBlock          | ✗     | ✔      |
| PrettyCompactNoEscapesMonoBlock | ✗     | ✔      |
| PrettySpace                     | ✗     | ✔      |
| PrettySpaceNoEscapes            | ✗     | ✔      |
| PrettySpaceMonoBlock            | ✗     | ✔      |
| PrettySpaceNoEscapesMonoBlock   | ✗     | ✔      |
| Prometheus                      | ✗     | ✔      |
| Protobuf                        | ✔     | ✔      |
| ProtobufSingle                  | ✔     | ✔      |
| Avro                            | ✔     | ✔      |
| AvroConfluent                   | ✔     | ✗      |
| Parquet                         | ✔     | ✔      |
| ParquetMetadata                 | ✔     | ✗      |
| Arrow                           | ✔     | ✔      |
| ArrowStream                     | ✔     | ✔      |
| ORC                             | ✔     | ✔      |
| One                             | ✔     | ✗      |
| RowBinary                       | ✔     | ✔      |
| RowBinaryWithNames              | ✔     | ✔      |
| RowBinaryWithNamesAndTypes      | ✔     | ✔      |
| RowBinaryWithDefaults           | ✔     | ✔      |
| Native                          | ✔     | ✔      |
| Null                            | ✗     | ✔      |
| XML                             | ✗     | ✔      |
| CapnProto                       | ✔     | ✔      |
| LineAsString                    | ✔     | ✔      |
| Regexp                          | ✔     | ✗      |
| RawBLOB                         | ✔     | ✔      |
| MsgPack                         | ✔     | ✔      |
| MySQLDump                       | ✔     | ✗      |
| Markdown                        | ✗     | ✔      |

詳細および例については、[ClickHouseの入力および出力データ形式](/docs/ja/interfaces/formats)をご覧ください。
