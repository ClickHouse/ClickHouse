---
alias: []
description: 'PrettyCompactMonoBlock 格式文档'
input_format: false
keywords: ['PrettyCompactMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyCompactMonoBlock
title: 'PrettyCompactMonoBlock'
doc_type: '参考'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 说明
</div>

与 [`PrettyCompact`](./PrettyCompact.md) 格式的不同之处在于，最多会缓冲 `10,000` 行数据，
然后以单个表的形式输出，而不是按[块](/zh/development/architecture#block)输出。

<div id="example-usage">
  ## 使用示例
</div>

<div id="format-settings">
  ## 格式设置
</div>

<PrettyFormatSettings />