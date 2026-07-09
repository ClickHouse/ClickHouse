---
alias: []
description: 'PrettyCompact フォーマットのリファレンス'
input_format: false
keywords: ['PrettyCompact']
output_format: true
slug: /interfaces/formats/PrettyCompact
title: 'PrettyCompact'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✗  | ✔  |       |

<div id="description">
  ## 説明
</div>

[`Pretty`](./Pretty.md) フォーマットとの違いは、行間に罫線を引いたグリッド形式でテーブルが表示される点です。
そのため、結果はよりコンパクトになります。

:::note
このフォーマットは、対話型モードのコマンドラインクライアントでデフォルトで使用されます。
:::

<div id="example-usage">
  ## 使用例
</div>

<div id="format-settings">
  ## フォーマット設定
</div>

<PrettyFormatSettings />