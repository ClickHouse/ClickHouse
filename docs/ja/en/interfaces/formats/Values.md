---
alias: []
description: 'Valuesフォーマットに関するドキュメント'
input_format: true
keywords: ['Values']
output_format: true
slug: /interfaces/formats/Values
title: 'Values'
doc_type: 'guide'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

`Values` フォーマットでは、各行は丸括弧で囲まれて出力されます。

* 行はカンマで区切られ、最後の行の末尾にはカンマは付きません。
* 丸括弧内の値もカンマで区切られます。
* 数値はクォートなしの10進形式で出力されます。
* Array は `[]` で出力されます。
* String、Date、および時刻を含む日付はクォート付きで出力されます。
* エスケープ規則とパースは [TabSeparated](TabSeparated/TabSeparated.md) フォーマットに似ています。

フォーマット時には余分なスペースは挿入されませんが、パース時にはスペースが許可され、読み飛ばされます (ただし、配列の値内のスペースは許可されません) 。
[`NULL`](/ja/sql-reference/syntax.md) は `NULL` として表されます。

`Values` フォーマットでデータを渡す際に、最低限エスケープする必要がある文字は次のとおりです。

* シングルクォート
* バックスラッシュ

これは `INSERT INTO t VALUES ...` で使用されるフォーマットですが、クエリ結果のフォーマットにも使用できます。

<div id="example-usage">
  ## 使用例
</div>

<div id="inserting-data">
  ### データの挿入
</div>

`Values` フォーマットは `INSERT` で使用されるため、`INSERT ... VALUES` ステートメントは
すでにこのフォーマットを使っています。`FORMAT Values` 句を明示的に指定することもでき、
行はストリームまたはファイルから入力できます。各行は括弧で囲まれた
カンマ区切りのタプルで、タプル同士もカンマで区切られます。

```sql title="Query"
CREATE TABLE t (id UInt32, name String, values Array(UInt32)) ENGINE = Memory;

INSERT INTO t FORMAT Values (1, 'a', [10, 20]), (2, 'b', [30]);

SELECT * FROM t ORDER BY id;
```

```response title="Response"
┌─id─┬─name─┬─values──┐
│  1 │ a    │ [10,20] │
│  2 │ b    │ [30]    │
└────┴──────┴─────────┘
```

<div id="using-expressions">
  ### 入力時に式を使用する
</div>

ほとんどの入力フォーマットとは異なり、`Values` では各フィールドで
リテラルだけでなく SQL 式も評価できます。これは
[`input_format_values_interpret_expressions`](#format-settings) で制御されており (既定で
有効) 、フィールドを高速なストリーミングパーサーで読み取れない場合は、
ClickHouse は SQL パーサーにフォールバックして、そのフィールドを式として解釈します。

```sql title="Query"
CREATE TABLE prices (item String, total UInt32) ENGINE = Memory;

INSERT INTO prices FORMAT Values ('apple', 3 * 4), ('pear', length('hello') + 10);

SELECT * FROM prices ORDER BY total;
```

```response title="Response"
┌─item──┬─total─┐
│ apple │    12 │
│ pear  │    15 │
└───────┴───────┘
```

<div id="selecting-data">
  ### データの選択
</div>

`Values` フォーマットは、クエリ結果のフォーマットにも使用できます。数値は
クォートなしで記述され、配列は`[]`、文字列とDateはシングルクォートで記述されます。
文字列内のシングルクォートとバックスラッシュはバックスラッシュでエスケープされ、
[`NULL`](/ja/sql-reference/syntax.md) は `NULL` と記述されます。

```sql title="Query"
SELECT 1 AS a, 'O''Reilly' AS b, NULL::Nullable(String) AS c FORMAT Values;
```

```response title="Response"
(1,'O\'Reilly',NULL)
```

<div id="format-settings">
  ## フォーマット設定
</div>

| 設定                                                                                                                                                          | 説明                                                                                                  | デフォルト  |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- | ------ |
| [`input_format_values_interpret_expressions`](../../operations/settings/settings-formats.md/#input_format_values_interpret_expressions)                     | フィールドをストリーミングパーサーでパースできない場合、SQLパーサーを実行し、SQL式として解釈を試みます。                                             | `true` |
| [`input_format_values_deduce_templates_of_expressions`](../../operations/settings/settings-formats.md/#input_format_values_deduce_templates_of_expressions) | フィールドをストリーミングパーサーでパースできない場合、SQLパーサーを実行してSQL式のテンプレートを推定し、そのテンプレートを使ってすべての行をパースしたうえで、すべての行の式の解釈を試みます。 | `true` |
| [`input_format_values_accurate_types_of_literals`](../../operations/settings/settings-formats.md/#input_format_values_accurate_types_of_literals)           | テンプレートを使用して式をパースおよび解釈する際に、オーバーフローや精度の問題を避けるため、リテラルの実際の型を確認します。                                      | `true` |