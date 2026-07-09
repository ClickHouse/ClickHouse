---
alias: []
description: 'PrettyCompact 格式说明文档'
input_format: false
keywords: ['PrettyCompact']
output_format: true
slug: /interfaces/formats/PrettyCompact
title: 'PrettyCompact'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 说明
</div>

与 [`Pretty`](./Pretty.md) 格式的不同之处在于，该格式会在表的各行之间绘制网格。
因此，结果会更紧凑。

:::note
在交互模式下，命令行客户端默认使用此格式。
:::

<div id="example-usage">
  ## 使用示例
</div>

<div id="format-settings">
  ## 格式设置
</div>

<PrettyFormatSettings />