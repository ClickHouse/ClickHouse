---
alias: []
description: 'PrettyCompactNoEscapes 格式文档'
input_format: false
keywords: ['PrettyCompactNoEscapes']
output_format: true
slug: /interfaces/formats/PrettyCompactNoEscapes
title: 'PrettyCompactNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 描述
</div>

与 [`PrettyCompact`](./PrettyCompact.md) 格式的区别在于，它不使用 [ANSI 转义序列](http://en.wikipedia.org/wiki/ANSI_escape_code)。
这对于在浏览器中显示该格式，以及使用 `watch` 命令行工具来说，都是必需的。

<div id="example-usage">
  ## 使用示例
</div>

<div id="format-settings">
  ## 格式设置
</div>

<PrettyFormatSettings />