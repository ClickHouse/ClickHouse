---
alias: []
description: 'CSVフォーマットに関するドキュメント'
input_format: true
keywords: ['CSV']
output_format: true
slug: /interfaces/formats/CSV
title: 'CSV'
doc_type: 'reference'
---

<div id="description">
  ## 説明
</div>

Comma Separated Valuesフォーマット ([RFC](https://tools.ietf.org/html/rfc4180)) 。
フォーマット時、行は二重引用符で囲まれます。文字列内の二重引用符は、二重引用符を2つ連続させた形で出力されます。
これ以外に文字のエスケープ規則はありません。

* Date および date-time は二重引用符で囲まれます。
* 数値は引用符なしで出力されます。
* 値は区切り文字で区切られ、デフォルトは `,` です。区切り文字は設定 [format&#95;csv&#95;delimiter](/ja/operations/settings/settings-formats.md/#format_csv_delimiter) で定義されます。
* 行は Unix のラインフィード (LF) で区切られます。
* Array は、CSV では次のようにシリアライズされます。
  * まず、Array は TabSeparatedフォーマットと同様に文字列へシリアライズされます
  * 生成された文字列は、二重引用符で囲まれて CSV に出力されます。
* CSV フォーマットの Tuple は個別のカラムとしてシリアライズされます (つまり、Tuple 内でのネストは失われます) 。

```bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

:::note
デフォルトの区切り文字は `,` です。
詳細は、設定 [format&#95;csv&#95;delimiter](/ja/operations/settings/settings-formats.md/#format_csv_delimiter) を参照してください。
:::

パース時には、すべての値をクォートあり・なしのいずれでもパースできます。ダブルクォートとシングルクォートの両方に対応しています。

行をクォートなしで並べることもできます。この場合、区切り文字または改行文字 (CR または LF) までパースされます。
ただし、RFC には準拠していませんが、クォートなしで行をパースする場合、先頭および末尾のスペースとタブは無視されます。
改行文字としては、Unix (LF) 、Windows (CR LF) 、Mac OS Classic (CR LF) の各形式をサポートしています。

`NULL` は、設定 [format&#95;csv&#95;null&#95;representation](/ja/operations/settings/settings-formats.md/#format_csv_null_representation) に従ってフォーマットされます (デフォルト値は `\N` です) 。

入力データでは、`ENUM` の値は名前または id として表現できます。
まず、入力値を `ENUM` 名に一致させようとします。
一致せず、かつ入力値が数値である場合は、この数値を `ENUM` id に一致させようとします。
入力データに `ENUM` id のみが含まれる場合は、`ENUM` のパースを最適化するため、設定 [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/ja/operations/settings/settings-formats.md/#input_format_csv_enum_as_number) を有効にすることを推奨します。

<div id="example-usage">
  ## 使用例
</div>

<div id="format-settings">
  ## フォーマット設定
</div>

| 設定                                                                                                                                                                                       | 説明                                                                              | デフォルト   | 注記                                                                                                                                                                              |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [format&#95;csv&#95;delimiter](/ja/operations/settings/settings-formats.md/#format_csv_delimiter)                                                                                           | CSVデータで区切り文字として扱う文字。                                                            | `,`     |                                                                                                                                                                                 |
| [format&#95;csv&#95;allow&#95;single&#95;quotes](/ja/operations/settings/settings-formats.md/#format_csv_allow_single_quotes)                                                               | シングルクォートで囲まれた文字列を許可します。                                                         | `true`  |                                                                                                                                                                                 |
| [format&#95;csv&#95;allow&#95;double&#95;quotes](/ja/operations/settings/settings-formats.md/#format_csv_allow_double_quotes)                                                               | ダブルクォートで囲まれた文字列を許可します。                                                          | `true`  |                                                                                                                                                                                 |
| [format&#95;csv&#95;null&#95;representation](/ja/operations/settings/settings-formats.md/#format_tsv_null_representation)                                                                   | CSVフォーマットでのカスタムNULL表現。                                                          | `\N`    |                                                                                                                                                                                 |
| [input&#95;format&#95;csv&#95;empty&#95;as&#95;default](/ja/operations/settings/settings-formats.md/#input_format_csv_empty_as_default)                                                     | CSV入力の空のフィールドをデフォルト値として扱います。                                                    | `true`  | 複雑なデフォルト式の場合は、[input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/ja/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) も有効にする必要があります。 |
| [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/ja/operations/settings/settings-formats.md/#input_format_csv_enum_as_number)                                                         | CSVフォーマットで挿入されるenum値をenumのインデックスとして扱います。                                        | `false` |                                                                                                                                                                                 |
| [input&#95;format&#95;csv&#95;use&#95;best&#95;effort&#95;in&#95;schema&#95;inference](/ja/operations/settings/settings-formats.md/#input_format_csv_use_best_effort_in_schema_inference)   | CSVフォーマットでスキーマを推論する際に、いくつかの調整とヒューリスティクスを使用します。無効にすると、すべてのフィールドがStringとして推論されます。 | `true`  |                                                                                                                                                                                 |
| [input&#95;format&#95;csv&#95;arrays&#95;as&#95;nested&#95;csv](/ja/operations/settings/settings-formats.md/#input_format_csv_arrays_as_nested_csv)                                         | CSVからArrayを読み取る際、その要素がネストされたCSVとしてシリアライズされ、さらに文字列に格納されていることを前提とします。             | `false` |                                                                                                                                                                                 |
| [output&#95;format&#95;csv&#95;crlf&#95;end&#95;of&#95;line](/ja/operations/settings/settings-formats.md/#output_format_csv_crlf_end_of_line)                                               | `true` に設定すると、CSV出力フォーマットの改行は `\n` ではなく `\r\n` になります。                           | `false` |                                                                                                                                                                                 |
| [input&#95;format&#95;csv&#95;skip&#95;first&#95;lines](/ja/operations/settings/settings-formats.md/#input_format_csv_skip_first_lines)                                                     | データの先頭で指定した行数をスキップします。                                                          | `0`     |                                                                                                                                                                                 |
| [input&#95;format&#95;csv&#95;detect&#95;header](/ja/operations/settings/settings-formats.md/#input_format_csv_detect_header)                                                               | CSVフォーマットで名前と型を含むヘッダーを自動検出します。                                                  | `true`  |                                                                                                                                                                                 |
| [input&#95;format&#95;csv&#95;skip&#95;trailing&#95;empty&#95;lines](/ja/operations/settings/settings-formats.md/#input_format_csv_skip_trailing_empty_lines)                               | データ末尾の空行をスキップします。                                                               | `false` |                                                                                                                                                                                 |
| [input&#95;format&#95;csv&#95;trim&#95;whitespaces](/ja/operations/settings/settings-formats.md/#input_format_csv_trim_whitespaces)                                                         | クォートされていないCSV文字列内のスペースとタブを削除します。                                                | `true`  |                                                                                                                                                                                 |
| [input&#95;format&#95;csv&#95;allow&#95;whitespace&#95;or&#95;tab&#95;as&#95;delimiter](/ja/operations/settings/settings-formats.md/#input_format_csv_allow_whitespace_or_tab_as_delimiter) | CSV文字列で、空白またはタブをフィールドの区切り文字として使用できるようにします。                                      | `false` |                                                                                                                                                                                 |
| [input&#95;format&#95;csv&#95;allow&#95;variable&#95;number&#95;of&#95;columns](/ja/operations/settings/settings-formats.md/#input_format_csv_allow_variable_number_of_columns)             | CSVフォーマットで可変数のカラムを許可し、余分なカラムを無視し、不足しているカラムにはデフォルト値を使用します。                       | `false` |                                                                                                                                                                                 |
| [input&#95;format&#95;csv&#95;use&#95;default&#95;on&#95;bad&#95;values](/ja/operations/settings/settings-formats.md/#input_format_csv_use_default_on_bad_values)                           | 不正な値によってCSVフィールドのデシリアライゼーションに失敗した場合、カラムにデフォルト値を設定できるようにします。                     | `false` |                                                                                                                                                                                 |
| [input&#95;format&#95;csv&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/ja/operations/settings/settings-formats.md/#input_format_csv_try_infer_numbers_from_strings)                 | スキーマ推論時に、文字列フィールドから数値を推論しようとします。                                                | `false` |                                                                                                                                                                                 |