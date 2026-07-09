---
alias: []
description: 'TemplateIgnoreSpaces フォーマットのリファレンス'
input_format: true
keywords: ['TemplateIgnoreSpaces']
output_format: false
slug: /interfaces/formats/TemplateIgnoreSpaces
title: 'TemplateIgnoreSpaces'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✗  |       |

<div id="description">
  ## 説明
</div>

[`Template`] に似ていますが、入力ストリーム内で区切り文字と値の間にある空白文字をスキップします。
ただし、フォーマット文字列に空白文字が含まれている場合は、入力ストリーム内でもそれらの文字が必要です。
また、空のプレースホルダー (`${}` または `${:None}`) を指定して、区切り文字の一部を個別のパーツに分割し、その間の空白を無視することもできます。
このようなプレースホルダーは、空白文字をスキップする目的でのみ使用されます。
すべての行でカラムの値の順序が同じであれば、このフォーマットで `JSON` を読み取ることができます。

:::note
このフォーマットは入力専用です。
:::

<div id="example-usage">
  ## 使用例
</div>

次のリクエストを使用すると、[JSON](/ja/interfaces/formats/JSON) フォーマットの出力例からデータを挿入できます。

```sql
INSERT INTO table_name 
SETTINGS
    format_template_resultset = '/some/path/resultset.format',
    format_template_row = '/some/path/row.format',
    format_template_rows_between_delimiter = ','
FORMAT TemplateIgnoreSpaces
```

```text title="/some/path/resultset.format"
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

```text title="/some/path/row.format"
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

<div id="format-settings">
  ## フォーマット設定
</div>
