---
alias: []
description: 'PrettyNoEscapes 格式说明'
input_format: false
keywords: ['PrettyNoEscapes']
output_format: true
slug: /interfaces/formats/PrettyNoEscapes
title: 'PrettyNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 描述
</div>

它与 [Pretty](/zh/interfaces/formats/Pretty) 的区别在于不使用 [ANSI-escape sequences](http://en.wikipedia.org/wiki/ANSI_escape_code)。
这对于在浏览器中显示该格式，以及使用 &#39;watch&#39; 命令行工具都是必需的。

<div id="example-usage">
  ## 使用示例
</div>

示例：

```bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

:::note
可通过 [HTTP 接口](/zh/interfaces/http) 在浏览器中显示该格式。
:::

<div id="format-settings">
  ## 格式设置
</div>

<PrettyFormatSettings />