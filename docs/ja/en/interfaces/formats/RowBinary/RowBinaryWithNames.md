---
description: 'RowBinaryWithNames フォーマットに関するドキュメント'
input_format: true
keywords: ['RowBinaryWithNames']
output_format: true
slug: /interfaces/formats/RowBinaryWithNames
title: 'RowBinaryWithNames'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

[`RowBinary`](./RowBinary.md) フォーマットに似ていますが、ヘッダーが追加されています。

* [`LEB128`](https://en.wikipedia.org/wiki/LEB128) でエンコードされたカラム数 (N) 。
* カラム名を指定する N 個の `String`。

<div id="example-usage">
  ## 使用例
</div>

<div id="format-settings">
  ## フォーマット設定
</div>

<RowBinaryFormatSettings />

:::note

* 設定 [`input_format_with_names_use_header`](/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header) が `1` に設定されている場合、入力データのカラムは名前に基づいてテーブルのカラムに対応付けられ、名前が不明なカラムはスキップされます。
* 設定 [`input_format_skip_unknown_fields`](/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) が `1` に設定されている場合。
  それ以外の場合は、最初の行がスキップされます。
  :::