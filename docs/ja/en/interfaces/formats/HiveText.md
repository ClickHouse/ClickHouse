---
alias: []
description: 'HiveTextフォーマットのドキュメント'
input_format: true
keywords: ['HiveText']
output_format: false
slug: /interfaces/formats/HiveText
title: 'HiveText'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✗  |       |

<div id="description">
  ## 説明
</div>

`HiveText` は、[Apache Hive](https://hive.apache.org/) の
テーブルで使用されるテキストのシリアライゼーションフォーマット
(Hive の `LazySimpleSerDe` が生成するフォーマット) を読み取ります。これは区切り付きテキスト
フォーマットで、[`CSV`](/ja/interfaces/formats/CSV) に似ています。フィールドは
Hive のデフォルトの `\x01` (Ctrl-A) 区切り文字で区切られます。このフィールド区切り文字は
[`input_format_hive_text_fields_delimiter`](#format-settings) で設定できます。

`HiveText` は入力専用のフォーマットです。データにはヘッダー行がなく、値は
宛先テーブルのカラムに位置に基づいて対応付けられます。そのため、カラム名と型はデータから
推論されるのではなく、テーブル (または明示的に指定された
構造) から取得されます。読み取り時、ClickHouse は
Date と時刻を best-effort モードでパースし ([`date_time_input_format`](/ja/operations/settings/formats#date_time_input_format) を参照) 、
省略された末尾のフィールドはカラムのデフォルト値で補完され、認識できないフィールドは
スキップされます。

フィールド内では、値は Hive のネストされた区切り文字ではなく、`CSV` と同じ
エスケープ規則を使ってパースされます。特に、
[`Array`](/ja/sql-reference/data-types/array) 型のカラムは角括弧付きの
表現 (たとえば `"['a','b','c']"`) から読み取られ、
Hive のコレクション区切り文字 `\x02` で区切られた値からは読み取られません。

:::note ネストされた区切り文字の設定は効果がありません
[`input_format_hive_text_collection_items_delimiter`](#format-settings) と
[`input_format_hive_text_map_keys_delimiter`](#format-settings) の設定は
互換性のために受け付けられていますが、現時点ではパース時に使用されません。
:::

デフォルトでは、行ごとにフィールド数が異なっていても許可されます (
[`input_format_hive_text_allow_variable_number_of_columns`](#format-settings) を参照) 。
テーブルよりフィールド数が少ない行では不足しているカラムが
デフォルト値で補完され、余分な末尾フィールドを持つ行ではその余分なフィールドはスキップされます。

<div id="example-usage">
  ## 使用例
</div>

以下の例では、[`input_format_hive_text_fields_delimiter`](#format-settings) を使用して
デフォルトのフィールド区切り文字をコンマ (`,`) に変更し、入力
ファイルを読みやすくしています。

<div id="reading-data">
  ### HiveTextファイルの読み込み
</div>

カンマ区切りのフィールドを含むファイル `hive_data.txt` があるとします。

```text title="hive_data.txt"
1,3
3,5,9
```

カラム名と型を定義するテーブルを作成し、`FORMAT HiveText` を使ってファイルを
そのテーブルに挿入します:

```sql title="Query"
CREATE TABLE test_tbl (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_tbl FROM INFILE 'hive_data.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_tbl;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 3 │ 0 │
│ 3 │ 5 │ 9 │
└───┴───┴───┘
```

最初の行 `1,3` にはフィールドが 2 つしかないため、欠けているカラム `c`
にはデフォルト値 `0` が設定されます。

<div id="variable-number-of-columns">
  ### 可変数のカラム
</div>

デフォルトで `input_format_hive_text_allow_variable_number_of_columns = 1` の場合、
テーブルのカラム数より多くのフィールドを持つ行では、末尾の余分なフィールドは
単にスキップされます：

```text title="hive_extras.txt"
1,2,3,4,5
6,7,8
```

```sql title="Query"
CREATE TABLE test_extras (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_extras FROM INFILE 'hive_extras.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_extras ORDER BY a;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 2 │ 3 │
│ 6 │ 7 │ 8 │
└───┴───┴───┘
```

代わりに `input_format_hive_text_allow_variable_number_of_columns = 0` を設定すると、
フィールド数が厳密にチェックされ、テーブルよりフィールド数の少ない行では
パース例外が発生します。

<div id="format-settings">
  ## フォーマット設定
</div>

| 設定                                                        | 説明                                                                             | デフォルト  |
| --------------------------------------------------------- | ------------------------------------------------------------------------------ | ------ |
| `input_format_hive_text_fields_delimiter`                 | Hive テキストファイル内のフィールド間の区切り文字                                                    | `\x01` |
| `input_format_hive_text_collection_items_delimiter`       | Hive テキストファイル内のコレクション (Array または map) の項目間の区切り文字。指定は受け付けられますが、現在はパース時に使用されません。 | `\x02` |
| `input_format_hive_text_map_keys_delimiter`               | Hive テキストファイル内の map のキーと値のペア間の区切り文字。指定は受け付けられますが、現在はパース時に使用されません。              | `\x03` |
| `input_format_hive_text_allow_variable_number_of_columns` | Hive Text入力で余分なカラムを無視し (ファイルのカラム数が想定より多い場合) 、欠落したフィールドはデフォルト値として扱います           | `1`    |