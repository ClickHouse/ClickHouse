---
alias: []
description: 'PrettyCompactNoEscapesMonoBlock 格式文档'
input_format: false
keywords: ['PrettyCompactNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyCompactNoEscapesMonoBlock
title: 'PrettyCompactNoEscapesMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 描述
</div>

与 [`PrettyCompactNoEscapes`](./PrettyCompactNoEscapes.md) 格式的不同之处在于：它会先缓冲最多 `10,000` 行，
然后将其作为一个表输出，而不是按[块](/zh/development/architecture#block)输出。

<div id="example-usage">
  ## 使用示例
</div>

<div id="format-settings">
  ## 格式设置
</div>

<PrettyFormatSettings />