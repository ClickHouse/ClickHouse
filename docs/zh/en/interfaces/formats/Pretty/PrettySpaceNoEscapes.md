---
alias: []
description: 'PrettySpaceNoEscapes 格式说明文档'
input_format: false
keywords: ['PrettySpaceNoEscapes']
output_format: true
slug: /interfaces/formats/PrettySpaceNoEscapes
title: 'PrettySpaceNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 说明
</div>

与 [`PrettySpace`](./PrettySpace.md) 格式的不同之处在于，该格式不使用 [ANSI 转义序列](http://en.wikipedia.org/wiki/ANSI_escape_code)。
这是为了能在浏览器中显示该格式，以及配合使用 `watch` 命令行工具。

<div id="example-usage">
  ## 使用示例
</div>

<div id="format-settings">
  ## 格式设置
</div>

<PrettyFormatSettings />