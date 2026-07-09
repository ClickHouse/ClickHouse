---
alias: []
description: 'PrettyCompactNoEscapesMonoBlock フォーマットのドキュメント'
input_format: false
keywords: ['PrettyCompactNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyCompactNoEscapesMonoBlock
title: 'PrettyCompactNoEscapesMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 説明
</div>

[`PrettyCompactNoEscapes`](./PrettyCompactNoEscapes.md) フォーマットとの違いは、最大 `10,000` 行をバッファに保持し、
それらを [blocks](/ja/development/architecture#block) 単位ではなく、1 つのテーブルとして出力する点です。

<div id="example-usage">
  ## 使用例
</div>

<div id="format-settings">
  ## フォーマット設定
</div>

<PrettyFormatSettings />