---
alias: []
description: 'RowBinaryWithDefaultsフォーマットのドキュメント'
input_format: true
keywords: ['RowBinaryWithDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithDefaults
title: 'RowBinaryWithDefaults'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 説明
</div>

[`RowBinary`](./RowBinary.md) フォーマットと似ていますが、各カラムの前に、デフォルト値を使用するかどうかを示す追加の1バイトがあります。

<div id="example-usage">
  ## 使用例
</div>

例:

```sql title="Query"
SELECT * FROM FORMAT('RowBinaryWithDefaults', 'x UInt32 default 42, y UInt32', x'010001000000')
```

```response title="Response"
┌──x─┬─y─┐
│ 42 │ 1 │
└────┴───┘
```

* カラム `x` には 1 バイトの `01` しかなく、これはデフォルト値を使用することを示します。このバイトの後に続くデータはありません。
* カラム `y` では、データはバイト `00` で始まります。これはそのカラムに実際の値があり、後続のデータ `01000000` から読み取る必要があることを示します。

<div id="format-settings">
  ## フォーマット設定
</div>

<RowBinaryFormatSettings />