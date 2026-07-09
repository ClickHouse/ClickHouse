---
alias: []
description: 'PrettySpaceNoEscapesフォーマットに関するドキュメント'
input_format: false
keywords: ['PrettySpaceNoEscapes']
output_format: true
slug: /interfaces/formats/PrettySpaceNoEscapes
title: 'PrettySpaceNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✗  | ✔  |       |

<div id="description">
  ## 説明
</div>

[`PrettySpace`](./PrettySpace.md) フォーマットとの違いは、[ANSIエスケープシーケンス](http://en.wikipedia.org/wiki/ANSI_escape_code) を使用しない点です。
これは、このフォーマットをブラウザーで表示したり、`watch` コマンドラインユーティリティーで使用したりするために必要です。

<div id="example-usage">
  ## 使用例
</div>

<div id="format-settings">
  ## フォーマット設定
</div>

<PrettyFormatSettings />