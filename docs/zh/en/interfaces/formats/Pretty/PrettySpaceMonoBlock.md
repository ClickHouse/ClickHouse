---
alias: []
description: 'PrettySpaceMonoBlock 格式文档'
input_format: false
keywords: ['PrettySpaceMonoBlock']
output_format: true
slug: /interfaces/formats/PrettySpaceMonoBlock
title: 'PrettySpaceMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 描述
</div>

与 [`PrettySpace`](./PrettySpace.md) 格式不同的是，它会先缓冲最多 `10,000` 行，
再将其作为一个单独的表输出，而不是按[块](/zh/development/architecture#block)输出。

<div id="example-usage">
  ## 使用示例
</div>

<div id="format-settings">
  ## 格式设置
</div>

<PrettyFormatSettings />