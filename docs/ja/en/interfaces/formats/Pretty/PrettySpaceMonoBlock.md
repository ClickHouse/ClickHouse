---
alias: []
description: 'PrettySpaceMonoBlockフォーマットのドキュメント'
input_format: false
keywords: ['PrettySpaceMonoBlock']
output_format: true
slug: /interfaces/formats/PrettySpaceMonoBlock
title: 'PrettySpaceMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✗  | ✔  |       |

<div id="description">
  ## 説明
</div>

[`PrettySpace`](./PrettySpace.md) フォーマットとは異なり、最大 `10,000` 行をバッファに保持したうえで、[blocks](/ja/development/architecture#block) ごとではなく、1 つのテーブルとして出力します。

<div id="example-usage">
  ## 使用例
</div>

<div id="format-settings">
  ## フォーマット設定
</div>

<PrettyFormatSettings />