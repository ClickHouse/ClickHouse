---
alias: []
description: 'PrettyCompactNoEscapes フォーマットのドキュメント'
input_format: false
keywords: ['PrettyCompactNoEscapes']
output_format: true
slug: /interfaces/formats/PrettyCompactNoEscapes
title: 'PrettyCompactNoEscapes'
doc_type: 'リファレンス'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 説明
</div>

[`PrettyCompact`](./PrettyCompact.md) フォーマットとの違いは、[ANSI エスケープシーケンス](http://en.wikipedia.org/wiki/ANSI_escape_code) を使用しないことです。
これは、このフォーマットをブラウザで表示したり、コマンドラインユーティリティの `watch` を使用したりするために必要です。

<div id="example-usage">
  ## 使用例
</div>

<div id="format-settings">
  ## フォーマット設定
</div>

<PrettyFormatSettings />