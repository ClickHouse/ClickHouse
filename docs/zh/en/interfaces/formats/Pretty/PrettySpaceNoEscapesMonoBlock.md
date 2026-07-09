---
alias: []
description: 'PrettySpaceNoEscapesMonoBlock 格式说明'
input_format: false
keywords: ['PrettySpaceNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettySpaceNoEscapesMonoBlock
title: 'PrettySpaceNoEscapesMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 说明
</div>

与 [`PrettySpaceNoEscapes`](./PrettySpaceNoEscapes.md) 格式的不同之处在于，它会先缓冲最多 `10,000` 行，
然后以单个表的形式输出，而不是按[块](/zh/development/architecture#block)输出。

<div id="example-usage">
  ## 使用示例
</div>

<div id="format-settings">
  ## 格式设置
</div>

<PrettyFormatSettings />