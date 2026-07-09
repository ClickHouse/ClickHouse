---
alias: []
description: 'PrettyNoEscapesMonoBlockフォーマットのリファレンスドキュメント'
input_format: false
keywords: ['PrettyNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyNoEscapesMonoBlock
title: 'PrettyNoEscapesMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 説明
</div>

[`PrettyNoEscapes`](./PrettyNoEscapes.md) フォーマットとの違いは、最大 `10,000` 行をバッファに保持し、
その後、ブロック単位ではなく 1 つのテーブルとして出力する点です。

<div id="example-usage">
  ## 使用例
</div>

<div id="format-settings">
  ## フォーマット設定
</div>

<PrettyFormatSettings />