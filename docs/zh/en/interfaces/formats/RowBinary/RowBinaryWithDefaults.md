---
alias: []
description: '关于 RowBinaryWithDefaults 格式的文档'
input_format: true
keywords: ['RowBinaryWithDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithDefaults
title: 'RowBinaryWithDefaults'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 说明
</div>

与 [`RowBinary`](./RowBinary.md) 格式类似，但在每列前会额外添加一个字节，用于指示是否应使用默认值。

<div id="example-usage">
  ## 使用示例
</div>

示例：

```sql title="Query"
SELECT * FROM FORMAT('RowBinaryWithDefaults', 'x UInt32 default 42, y UInt32', x'010001000000')
```

```response title="Response"
┌──x─┬─y─┐
│ 42 │ 1 │
└────┴───┘
```

* 对于列 `x`，只有一个字节 `01`，表示应使用默认值，且该字节之后不再有其他数据。
* 对于列 `y`，数据以字节 `00` 开头，表示该列具有实际值，应从后续数据 `01000000` 中读取。

<div id="format-settings">
  ## 格式设置
</div>

<RowBinaryFormatSettings />