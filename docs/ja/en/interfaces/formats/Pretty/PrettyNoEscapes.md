---
alias: []
description: 'PrettyNoEscapesフォーマットのドキュメント'
input_format: false
keywords: ['PrettyNoEscapes']
output_format: true
slug: /interfaces/formats/PrettyNoEscapes
title: 'PrettyNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 説明
</div>

[Pretty](/ja/interfaces/formats/Pretty) とは異なり、[ANSIエスケープシーケンス](http://en.wikipedia.org/wiki/ANSI_escape_code) は使用されません。
これは、このフォーマットをブラウザーで表示したり、`watch` コマンドラインユーティリティを使用したりするために必要です。

<div id="example-usage">
  ## 使用例
</div>

例:

```bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

:::note
ブラウザでこのフォーマットを表示するには、[HTTP interface](/ja/interfaces/http) を使用できます。
:::

<div id="format-settings">
  ## フォーマット設定
</div>

<PrettyFormatSettings />