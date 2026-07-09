---
alias: []
description: 'PrettyMonoBlockフォーマットのドキュメント'
input_format: false
keywords: ['PrettyMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyMonoBlock
title: 'PrettyMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✗  | ✔  |       |

<div id="description">
  ## 説明
</div>

[`Pretty`](/ja/interfaces/formats/Pretty) フォーマットとの違いは、最大 `10,000` 行までをバッファリングし、
その後、[ブロック](/ja/development/architecture#block) ごとではなく、1 つのテーブルとして出力する点です。

<div id="example-usage">
  ## 使用例
</div>

<div id="format-settings">
  ## フォーマット設定
</div>

<PrettyFormatSettings />