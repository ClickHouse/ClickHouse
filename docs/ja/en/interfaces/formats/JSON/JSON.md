---
alias: []
description: 'JSONフォーマットに関するドキュメント'
input_format: true
keywords: ['JSON']
output_format: true
slug: /interfaces/formats/JSON
title: 'JSON'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

`JSON` フォーマットは、データを JSON フォーマットで読み込み、出力します。

`JSON` フォーマットは、次の内容を返します。

| Parameter                    | Description                                                                                                                                                                                                         |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `meta`                       | カラム名と型。                                                                                                                                                                                                             |
| `data`                       | データテーブル。                                                                                                                                                                                                            |
| `rows`                       | 出力される行の総数。                                                                                                                                                                                                          |
| `rows_before_limit_at_least` | LIMIT がなかった場合の行数の下限推定値。クエリに LIMIT が含まれている場合にのみ出力されます。この推定値は、limit transform の前にクエリパイプラインで処理されたデータブロックから計算されますが、その後 limit transform によって破棄されることがあります。ブロックがクエリパイプライン内で limit transform に到達していない場合、そのブロックはこの推定に含まれません。 |
| `statistics`                 | `elapsed`、`rows_read`、`bytes_read` などの統計情報。                                                                                                                                                                         |
| `totals`                     | 合計値 (WITH TOTALS を使用している場合) 。                                                                                                                                                                                       |
| `extremes`                   | 極値 (extremes が 1 に設定されている場合) 。                                                                                                                                                                                      |

`JSON` は JavaScript と互換性があります。これを保証するため、一部の文字は追加でエスケープされます。

* スラッシュ `/` は `\/` としてエスケープされます
* 一部のブラウザーで問題を引き起こす代替改行 `U+2028` と `U+2029` は、`\uXXXX` としてエスケープされます。
* ASCII 制御文字はエスケープされます。バックスペース、改ページ、行送り、復帰、水平タブはそれぞれ `\b`、`\f`、`\n`、`\r`、`\t` に置き換えられ、さらに 00-1F の範囲にある残りのバイトも `\uXXXX` シーケンスで表されます。
* 無効な UTF-8 シーケンスは置換文字 � に置き換えられるため、出力テキストは有効な UTF-8 シーケンスのみで構成されます。

JavaScript との互換性のため、Int64 および UInt64 の整数はデフォルトで二重引用符で囲まれます。
引用符を外すには、設定パラメーター [`output_format_json_quote_64bit_integers`](/ja/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers) を `0` に設定します。

ClickHouse は [NULL](/ja/sql-reference/syntax.md) をサポートしており、JSON 出力では `null` として表示されます。出力で `+nan`、`-nan`、`+inf`、`-inf` の値を有効にするには、[output&#95;format&#95;json&#95;quote&#95;denormals](/ja/operations/settings/settings-formats.md/#output_format_json_quote_denormals) を `1` に設定します。

<div id="example-usage">
  ## 使用例
</div>

例:

```sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },
                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                {
                        "num": 42,
                        "str": "hello",
                        "arr": [0,1]
                },
                {
                        "num": 43,
                        "str": "hello",
                        "arr": [0,1,2]
                },
                {
                        "num": 44,
                        "str": "hello",
                        "arr": [0,1,2,3]
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001137687,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

<div id="format-settings">
  ## フォーマット設定
</div>

JSON入力フォーマットでは、設定 [`input_format_json_validate_types_from_metadata`](/ja/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) が `1` に設定されている場合、
入力データ内のメタデータの型が、テーブル内の対応するカラムの型と比較されます。

<div id="see-also">
  ## 関連項目
</div>

* [JSONEachRow](/ja/interfaces/formats/JSONEachRow) フォーマット
* [output&#95;format&#95;json&#95;array&#95;of&#95;rows](/ja/operations/settings/settings-formats.md/#output_format_json_array_of_rows) 設定