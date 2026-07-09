---
alias: []
description: 'PrettyCompactMonoBlockフォーマットに関するドキュメント'
input_format: false
keywords: ['PrettyCompactMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyCompactMonoBlock
title: 'PrettyCompactMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✗  | ✔  |       |

<div id="description">
  ## 説明
</div>

[`PrettyCompact`](./PrettyCompact.md) フォーマットとの違いは、最大 `10,000` 行をバッファに保持し、
その後 [ブロック](/ja/development/architecture#block)単位ではなく、1 つの表として出力する点です。

<div id="example-usage">
  ## 使用例
</div>

<div id="format-settings">
  ## フォーマット設定
</div>

<PrettyFormatSettings />