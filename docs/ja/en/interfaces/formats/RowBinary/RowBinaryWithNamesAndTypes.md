---
alias: []
description: 'RowBinaryWithNamesAndTypes フォーマットに関するドキュメント'
input_format: true
keywords: ['RowBinaryWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/RowBinaryWithNamesAndTypes
title: 'RowBinaryWithNamesAndTypes'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 説明
</div>

[RowBinary](./RowBinary.md)フォーマットに似ていますが、以下のヘッダーが追加されています。

* [`LEB128`](https://en.wikipedia.org/wiki/LEB128)でエンコードされたカラム数 (N)。
* カラム名を指定する `String` が N 個。
* カラム型を指定する `String` が N 個。

<div id="example-usage">
  ## 使用例
</div>

<div id="format-settings">
  ## フォーマット設定
</div>

<RowBinaryFormatSettings />

:::note
設定 [`input_format_with_names_use_header`](/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header) が 1 に設定されている場合、
入力データのカラムは名前に基づいてテーブルのカラムに対応付けられ、設定 [input&#95;format&#95;skip&#95;unknown&#95;fields](/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) が 1 に設定されていれば、不明な名前のカラムはスキップされます。
それ以外の場合は、1 行目がスキップされます。
設定 [`input_format_with_types_use_header`](/ja/operations/settings/settings-formats.md/#input_format_with_types_use_header) が `1` に設定されている場合、
入力データの型は、テーブル内の対応するカラムの型と比較されます。それ以外の場合は、2 行目がスキップされます。
:::