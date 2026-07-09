---
alias: []
description: 'PrettyNoEscapesMonoBlock 格式说明文档'
input_format: false
keywords: ['PrettyNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyNoEscapesMonoBlock
title: 'PrettyNoEscapesMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 说明
</div>

与 [`PrettyNoEscapes`](./PrettyNoEscapes.md) 格式的区别在于，它会先缓冲最多 `10,000` 行，
然后一次性以单个表的形式输出，而不是按块输出。

<div id="example-usage">
  ## 使用示例
</div>

<div id="format-settings">
  ## 格式设置
</div>

<PrettyFormatSettings />