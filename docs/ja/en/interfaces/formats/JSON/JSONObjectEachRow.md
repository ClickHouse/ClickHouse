---
alias: []
description: 'JSONObjectEachRow フォーマットに関するドキュメント'
input_format: true
keywords: ['JSONObjectEachRow']
output_format: true
slug: /interfaces/formats/JSONObjectEachRow
title: 'JSONObjectEachRow'
doc_type: 'reference'
---

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 説明
</div>

このフォーマットでは、すべてのデータが 1 つのJSONオブジェクトとして表され、各行は [`JSONEachRow`](./JSONEachRow.md) フォーマットと同様に、そのオブジェクト内の個別のフィールドとして表されます。

<div id="example-usage">
  ## 使用例
</div>

<div id="basic-example">
  ### 基本的な例
</div>

次の JSON があるとします。

```json
{
  "row_1": {"num": 42, "str": "hello", "arr":  [0,1]},
  "row_2": {"num": 43, "str": "hello", "arr":  [0,1,2]},
  "row_3": {"num": 44, "str": "hello", "arr":  [0,1,2,3]}
}
```

オブジェクト名をカラムの値として使用するには、特別な設定 [`format_json_object_each_row_column_for_object_name`](/ja/operations/settings/settings-formats.md/#format_json_object_each_row_column_for_object_name) を使用できます。
この設定の値にはカラム名を指定します。このカラムは、生成されるオブジェクト内で各行の JSON キーとして使用されます。

<div id="output">
  #### 出力
</div>

2 つのカラムを持つ `test` テーブルがあるとします。

```text
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

`JSONObjectEachRow` フォーマットで出力し、`format_json_object_each_row_column_for_object_name` 設定を使いましょう。

```sql title="Query"
SELECT * FROM test SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```json title="Response"
{
    "first_obj": {"number": 1},
    "second_obj": {"number": 2},
    "third_obj": {"number": 3}
}
```

<div id="input">
  #### 入力
</div>

前の例の出力を、`data.json` という名前のファイルに保存したとします。

```sql title="Query"
SELECT * FROM file('data.json', JSONObjectEachRow, 'object_name String, number UInt64') SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

スキーマ推論にも使用できます:

```sql title="Query"
DESCRIBE file('data.json', JSONObjectEachRow) SETTING format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─name────────┬─type────────────┐
│ object_name │ String          │
│ number      │ Nullable(Int64) │
└─────────────┴─────────────────┘
```

<div id="json-inserting-data">
  ### データを挿入する
</div>

```sql title="Query"
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

ClickHouse では、次のことが可能です。

* オブジェクト内のキー・バリューのペアは、どのような順序でも指定できます。
* 一部の値は省略できます。

ClickHouse は、要素間の空白やオブジェクトの後ろのカンマを無視します。すべてのオブジェクトを 1 行で指定することもできます。改行で区切る必要はありません。

<div id="omitted-values-processing">
  #### 省略された値の処理
</div>

ClickHouse は、省略された値を、対応する [データ型](/ja/sql-reference/data-types/index.md) のデフォルト値で補います。

`DEFAULT expr` が指定されている場合、ClickHouse は [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/ja/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) 設定に応じて、異なる補完ルールを使用します。

次のテーブルを考えます。

```sql title="Query"
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

* `input_format_defaults_for_omitted_fields = 0` の場合、`x` と `a` のデフォルト値は `0` です (`UInt32` データ型のデフォルト値であるため) 。
* `input_format_defaults_for_omitted_fields = 1` の場合、`x` のデフォルト値は `0` ですが、`a` のデフォルト値は `x * 2` になります。

:::note
`input_format_defaults_for_omitted_fields = 1` を指定してデータを挿入すると、`input_format_defaults_for_omitted_fields = 0` を指定した場合の挿入と比べて、ClickHouse はより多くの計算リソースを消費します。
:::

<div id="json-selecting-data">
  ### データの選択
</div>

例として、`UserActivity` テーブルを考えてみましょう。

```response
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

クエリ `SELECT * FROM UserActivity FORMAT JSONEachRow` は次の結果を返します：

```response
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

[JSON](/ja/interfaces/formats/JSON)フォーマットとは異なり、無効な UTF-8 シーケンスは置換されません。値は `JSON` と同じ方法でエスケープされます。

:::info
文字列には任意のバイト列を出力できます。テーブル内のデータを情報を失うことなく JSON としてフォーマットできることが確実な場合は、[`JSONEachRow`](./JSONEachRow.md)フォーマットを使用してください。
:::

<div id="jsoneachrow-nested">
  ### Nested 構造の利用
</div>

[`Nested`](/ja/sql-reference/data-types/nested-data-structures/index.md) データ型のカラムを持つテーブルがある場合、同じ構造の JSON データを挿入できます。この機能は、[input&#95;format&#95;import&#95;nested&#95;json](/ja/operations/settings/settings-formats.md/#input_format_import_nested_json) 設定を有効にすることで使用できます。

たとえば、次のテーブルを考えます。

```sql title="Query"
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

`Nested` データ型の説明にあるように、ClickHouse はネストされた構造の各部分をそれぞれ別個のカラムとして扱います (このテーブルでは `n.s` と `n.i`) 。データは次のように挿入できます。

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

データを階層構造のJSONオブジェクトとして挿入するには、[`input_format_import_nested_json=1`](/ja/operations/settings/settings-formats.md/#input_format_import_nested_json)を設定します。

```json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

この設定がない場合、ClickHouseは例外をスローします。

```sql title="Query"
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

```response title="Response"
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

```response title="Response"
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

```sql title="Query"
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

```response title="Response"
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

<div id="format-settings">
  ## フォーマット設定
</div>

| 設定                                                                                                                                                                           | 説明                                                                                                                       | デフォルト    | 注記                                                                                                                                                                |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`input_format_import_nested_json`](/ja/operations/settings/settings-formats.md/#input_format_import_nested_json)                                                               | ネストされた JSON データをネストされたテーブルにマッピングします (JSONEachRow フォーマットで機能します) 。                                                         | `false`  |                                                                                                                                                                   |
| [`input_format_json_read_bools_as_numbers`](/ja/operations/settings/settings-formats.md/#input_format_json_read_bools_as_numbers)                                               | JSON 入力フォーマットで、bool 値を数値として解析できるようにします。                                                                                  | `true`   |                                                                                                                                                                   |
| [`input_format_json_read_bools_as_strings`](/ja/operations/settings/settings-formats.md/#input_format_json_read_bools_as_strings)                                               | JSON入力フォーマットで、bool を String として解析できるようにします。                                                                              | `true`   |                                                                                                                                                                   |
| [`input_format_json_read_numbers_as_strings`](/ja/operations/settings/settings-formats.md/#input_format_json_read_numbers_as_strings)                                           | JSON入力フォーマットで、数値を String として解析できるようにします。                                                                                 | `true`   |                                                                                                                                                                   |
| [`input_format_json_read_arrays_as_strings`](/ja/operations/settings/settings-formats.md/#input_format_json_read_arrays_as_strings)                                             | JSON入力フォーマットで、JSON配列を String として解析できるようにします。                                                                             | `true`   |                                                                                                                                                                   |
| [`input_format_json_read_objects_as_strings`](/ja/operations/settings/settings-formats.md/#input_format_json_read_objects_as_strings)                                           | JSON input formatsで、JSON オブジェクトを文字列として解析できるようにします。                                                                       | `true`   |                                                                                                                                                                   |
| [`input_format_json_named_tuples_as_objects`](/ja/operations/settings/settings-formats.md/#input_format_json_named_tuples_as_objects)                                           | named tuple のカラムを JSON オブジェクトとして解析します。                                                                                   | `true`   |                                                                                                                                                                   |
| [`input_format_json_try_infer_numbers_from_strings`](/ja/operations/settings/settings-formats.md/#input_format_json_try_infer_numbers_from_strings)                             | スキーマ推論時に、文字列フィールドから数値を推論することを試みます。                                                                                       | `false`  |                                                                                                                                                                   |
| [`input_format_json_try_infer_named_tuples_from_objects`](/ja/operations/settings/settings-formats.md/#input_format_json_try_infer_named_tuples_from_objects)                   | スキーマ推論時に、JSON オブジェクトから名前付き Tuple を推論することを試みます。                                                                           | `true`   |                                                                                                                                                                   |
| [`input_format_json_infer_incomplete_types_as_strings`](/ja/operations/settings/settings-formats.md/#input_format_json_infer_incomplete_types_as_strings)                       | JSON 入力フォーマットでのスキーマ推論時に、Null のみ、または空のオブジェクト/配列しか含まないキーには String 型を使用します。                                                 | `true`   |                                                                                                                                                                   |
| [`input_format_json_defaults_for_missing_elements_in_named_tuple`](/ja/operations/settings/settings-formats.md/#input_format_json_defaults_for_missing_elements_in_named_tuple) | 名前付き Tuple のパース時に、JSON オブジェクト内で欠けている要素にデフォルト値を挿入します。                                                                     | `true`   |                                                                                                                                                                   |
| [`input_format_json_ignore_unknown_keys_in_named_tuple`](/ja/operations/settings/settings-formats.md/#input_format_json_ignore_unknown_keys_in_named_tuple)                     | named tuple の JSON object 内の不明なキーを無視します。                                                                                 | `false`  |                                                                                                                                                                   |
| [`input_format_json_compact_allow_variable_number_of_columns`](/ja/operations/settings/settings-formats.md/#input_format_json_compact_allow_variable_number_of_columns)         | JSONCompact/JSONCompactEachRow フォーマットで可変数のカラムを許可し、余分なカラムは無視し、不足しているカラムにはデフォルト値を使用します。                                    | `false`  |                                                                                                                                                                   |
| [`input_format_json_throw_on_bad_escape_sequence`](/ja/operations/settings/settings-formats.md/#input_format_json_throw_on_bad_escape_sequence)                                 | JSON string に不正な Escape sequences が含まれている場合は例外をスローします。無効にすると、不正な Escape sequences はデータ内でそのまま保持されます。                      | `true`   |                                                                                                                                                                   |
| [`input_format_json_empty_as_default`](/ja/operations/settings/settings-formats.md/#input_format_json_empty_as_default)                                                         | JSON入力内の空のフィールドをデフォルト値として扱います。                                                                                           | `false`. | 複雑なデフォルト式を使用する場合は、[`input_format_defaults_for_omitted_fields`](/ja/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) も有効にする必要があります。 |
| [`output_format_json_quote_64bit_integers`](/ja/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers)                                               | JSON出力フォーマットで64ビット整数を引用符で囲むかどうかを制御します。                                                                                   | `true`   |                                                                                                                                                                   |
| [`output_format_json_quote_64bit_floats`](/ja/operations/settings/settings-formats.md/#output_format_json_quote_64bit_floats)                                                   | JSON出力フォーマットで64ビット浮動小数点数を引用符で囲むかどうかを制御します。                                                                               | `false`  |                                                                                                                                                                   |
| [`output_format_json_quote_denormals`](/ja/operations/settings/settings-formats.md/#output_format_json_quote_denormals)                                                         | JSON出力フォーマットで &#39;+nan&#39;、&#39;-nan&#39;、&#39;+inf&#39;、&#39;-inf&#39; を出力できるようにします。                                  | `false`  |                                                                                                                                                                   |
| [`output_format_json_quote_decimals`](/ja/operations/settings/settings-formats.md/#output_format_json_quote_decimals)                                                           | JSON出力フォーマットでDecimal値を引用符で囲むかどうかを制御します。                                                                                  | `false`  |                                                                                                                                                                   |
| [`output_format_json_escape_forward_slashes`](/ja/operations/settings/settings-formats.md/#output_format_json_escape_forward_slashes)                                           | JSON出力フォーマットで文字列出力のフォワードスラッシュをエスケープするかどうかを制御します。                                                                         | `true`   |                                                                                                                                                                   |
| [`output_format_json_named_tuples_as_objects`](/ja/operations/settings/settings-formats.md/#output_format_json_named_tuples_as_objects)                                         | named tuple のカラムを JSON オブジェクトとしてシリアライズします。                                                                               | `true`   |                                                                                                                                                                   |
| [`output_format_json_array_of_rows`](/ja/operations/settings/settings-formats.md/#output_format_json_array_of_rows)                                                             | JSONEachRow(Compact) フォーマットですべての行を JSON 配列として出力します。                                                                      | `false`  |                                                                                                                                                                   |
| [`output_format_json_validate_utf8`](/ja/operations/settings/settings-formats.md/#output_format_json_validate_utf8)                                                             | JSON 出力フォーマットでの UTF-8 シーケンスの検証を有効にします (なお、JSON/JSONCompact/JSONColumnsWithMetadata フォーマットには影響しません。これらは常に UTF-8 を検証します) 。 | `false`  |                                                                                                                                                                   |